import logging
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

from abstract_customer import get_customer_json, get_lead_json
from amocrm import Customer, COMPANIES, CONTACTS, Lead, amocrm_get
from models import db, AbstractCustomer

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
ENTITY_FIELDS = ("amo_customers", "amo_leads", "amo_contacts", "amo_companies")

def _general_id(item):
    return (item or {}).get("general", {}).get("id")


def _general_login(item):
    return (item or {}).get("general", {}).get("login")


def _general_name(item):
    return (item or {}).get("general", {}).get("name") or ""


def _merge_items(left, right):
    """Merge JSON entities by CRM id; the newer representation wins."""
    result = {}
    for item in (left or []) + (right or []):
        entity_id = _general_id(item)
        if entity_id is None:
            continue
        previous = result.get(entity_id, {})
        merged = dict(previous)
        merged.update(item)
        general = dict(previous.get("general") or {})
        general.update(item.get("general") or {})
        merged["general"] = general
        result[entity_id] = merged
    return list(result.values())

def _embedded_json(items):
    """Normalize embedded CRM contacts/companies to the legacy JSON shape."""
    result = []
    for item in items or []:
        if not isinstance(item, dict) or item.get("id") is None:
            continue
        result.append({
            "general": {"id": item["id"], "name": item.get("name") or ""},
            "api_value": item,
        })
    return result


def _build_indexes(rows):
    by_customer_id = {}
    by_lead_id = {}
    by_customer_login = defaultdict(set)
    by_lead_login = defaultdict(set)
    for row in rows:
        for item in row.amo_customers or []:
            if _general_id(item) is not None:
                by_customer_id[_general_id(item)] = row
            if _general_login(item):
                by_customer_login[_general_login(item)].add(row)
        for item in row.amo_leads or []:
            if _general_id(item) is not None:
                by_lead_id[_general_id(item)] = row
            if _general_login(item):
                by_lead_login[_general_login(item)].add(row)
    return by_customer_id, by_lead_id, by_customer_login, by_lead_login

def _unique_login(index, login):
    matches = index.get(login, set()) if login else set()
    if len(matches) != 1:
        if len(matches) > 1:
            logger.warning("Ambiguous CRM login %r: %d aggregate rows", login, len(matches))
        return None
    return next(iter(matches))



def _remove_index(index, key, row):
    if not key:
        return
    matches = index.get(key)
    if matches:
        matches.discard(row)
        if not matches:
            index.pop(key, None)

def _add_index(index, key, row):
    if key:
        index[key].add(row)


def _remove_row_from_indexes(row, customer_login_index, lead_login_index):
    for index in (customer_login_index, lead_login_index):
        for key in list(index):
            index[key].discard(row)
            if not index[key]:
                del index[key]

def _merge_rows(target, source, indexes, seen):
    if target is source:
        return target
    for field in ENTITY_FIELDS:
        setattr(target, field, _merge_items(getattr(target, field), getattr(source, field)))
        flag_modified(target, field)
    db.session.delete(source)
    customer_id, lead_id, customer_login, lead_login = indexes
    for mapping in (customer_id, lead_id):
        for key, value in list(mapping.items()):
            if value is source:
                mapping[key] = target
    _remove_row_from_indexes(source, customer_login, lead_login)
    source_seen = seen.pop(source.id, None)
    if source_seen:
        target_seen = seen.setdefault(target.id, {
            "amo_customers": set(), "amo_leads": set(),
            "amo_contacts": set(), "amo_companies": set(),
        })
        for field in ENTITY_FIELDS:
            target_seen[field].update(source_seen[field])
    for item in target.amo_customers or []:
        if _general_login(item):
            customer_login[_general_login(item)].add(target)
    for item in target.amo_leads or []:
        if _general_login(item):
            lead_login[_general_login(item)].add(target)
    return target

def _upsert_json(items, value):
    entity_id = value["general"]["id"]
    for index, current in enumerate(items):
        if _general_id(current) == entity_id:
            items[index] = value
            return
    items.append(value)


def _mark_seen(seen, row, field, entity_ids):
    bucket = seen.setdefault(row.id, {
        "amo_customers": set(), "amo_leads": set(),
        "amo_contacts": set(), "amo_companies": set(),
    })
    bucket[field].update(entity_ids)


def _cleanup(rows, seen):
    """Apply authoritative snapshot cleanup only after both CRM streams finish."""
    for row in rows:
        keep = seen.get(row.id, {
            "amo_customers": set(), "amo_leads": set(),
            "amo_contacts": set(), "amo_companies": set(),
        })
        for field in ENTITY_FIELDS:
            old = getattr(row, field) or []
            filtered = [
                item for item in old
                if _general_id(item) in keep[field]
                and "удалить" not in _general_name(item).casefold()
            ]
            if filtered != old:
                setattr(row, field, filtered)
                flag_modified(row, field)
        if not (row.amo_customers or row.amo_leads):
            db.session.delete(row)

def collect_leads_customers(is_full=True):
    session = db.session()
    rows = session.scalars(select(AbstractCustomer)).all()
    previous_expire_setting = session.expire_on_commit
    session.expire_on_commit = False
    indexes = _build_indexes(rows)
    by_customer_id, by_lead_id, by_customer_login, by_lead_login = indexes
    seen = {}
    pending = 0

    try:
        customer_stream = amocrm_get(Customer, _with=f"{COMPANIES},{CONTACTS}")
        logger.info("Collecting CRM customers (full=%s)", is_full)
        for customer in customer_stream:
            if not is_full and customer.status_id == 111349:
                continue
            login = customer.get_field("Логин")
            if not login:
                continue

            row = by_customer_id.get(customer.id)
            if row is None:
                row = _unique_login(by_lead_login, login)
            if row is None:
                row = AbstractCustomer(amo_customers=[])
                session.add(row)
                session.flush()
                rows.append(row)

            previous_customer = next(
                (item for item in (row.amo_customers or [])
                 if _general_id(item) == customer.id), None
            )
            if previous_customer:
                _remove_index(by_customer_login, _general_login(previous_customer), row)
            value = get_customer_json(customer)
            row.amo_customers = _merge_items(row.amo_customers, [value])
            row.amo_contacts = _merge_items(
                row.amo_contacts, _embedded_json(customer.contacts)
            )
            row.amo_companies = _merge_items(
                row.amo_companies, _embedded_json(customer.companies)
            )
            flag_modified(row, "amo_customers")
            flag_modified(row, "amo_contacts")
            flag_modified(row, "amo_companies")

            by_customer_id[customer.id] = row
            _add_index(by_customer_login, login, row)
            _mark_seen(seen, row, "amo_customers", [customer.id])
            _mark_seen(seen, row, "amo_contacts",
                       [item["id"] for item in customer.contacts or [] if item.get("id") is not None])
            _mark_seen(seen, row, "amo_companies",
                       [item["id"] for item in customer.companies or [] if item.get("id") is not None])
            pending += 1
            if pending >= BATCH_SIZE:
                session.commit()
                pending = 0

        logger.info("Collecting CRM leads (full=%s)", is_full)
        lead_stream = amocrm_get(Lead, _with=f"{CONTACTS}")
        for lead in lead_stream:
            if not is_full and lead.status_id == 143:
                continue
            login = lead.get_field("Логин")
            row_by_id = by_lead_id.get(lead.id)
            row_by_login = _unique_login(by_customer_login, login)
            if row_by_id is not None:
                row = row_by_id
                if row_by_login is not None and row_by_login is not row:
                    row = _merge_rows(row, row_by_login, indexes, seen)
                    rows = [candidate for candidate in rows if candidate is not row_by_login]
            else:
                row = row_by_login

            if row is None:
                row = AbstractCustomer(amo_leads=[])
                session.add(row)
                session.flush()
                rows.append(row)

            previous_lead = next(
                (item for item in (row.amo_leads or [])
                 if _general_id(item) == lead.id), None
            )
            if previous_lead:
                _remove_index(by_lead_login, _general_login(previous_lead), row)
            row.amo_leads = _merge_items(row.amo_leads, [get_lead_json(lead)])
            if login:
                _add_index(by_lead_login, login, row)
            row.amo_contacts = _merge_items(
                row.amo_contacts, _embedded_json(lead.contacts)
            )
            flag_modified(row, "amo_leads")
            flag_modified(row, "amo_contacts")
            by_lead_id[lead.id] = row
            _mark_seen(seen, row, "amo_leads", [lead.id])
            _mark_seen(seen, row, "amo_contacts",
                       [item["id"] for item in lead.contacts or [] if item.get("id") is not None])
            pending += 1
            if pending >= BATCH_SIZE:
                session.commit()
                pending = 0

        session.commit()
        if not is_full:
            return

        _cleanup(rows, seen)
        session.commit()
        logger.info("CRM synchronization and cleanup completed")
    except Exception:
        session.rollback()
        logger.exception("CRM synchronization failed; destructive cleanup was skipped")
        raise
    finally:
        session.expire_on_commit = previous_expire_setting