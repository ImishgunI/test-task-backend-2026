import logging
from collections import defaultdict

from sqlalchemy import inspect, select

from abstract_customer import get_customer_json, get_lead_json
from amocrm import Customer, COMPANIES, CONTACTS, Lead, amocrm_get
from models import db, AbstractCustomer

logger = logging.getLogger(__name__)

FLUSH_BATCH_SIZE = 500
ENTITY_FIELDS = (
    "amo_customers",
    "amo_leads",
    "amo_contacts",
    "amo_companies",
)

def _general_id(item):
    return (item or {}).get("general", {}).get("id")


def _general_login(item):
    return (item or {}).get("general", {}).get("login")


def _general_name(item):
    return (item or {}).get("general", {}).get("name") or ""


def _merge_items(left, right):
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
    result = []

    for item in items or []:
        if not isinstance(item, dict) or item.get("id") is None:
            continue

        result.append({
            "general": {
                "id": item["id"],
                "name": item.get("name") or "",
            },
            "api_value": item,
        })

    return result

def _add_unique_id(index, entity_id, row, entity_type):
    if entity_id is None:
        return

    previous = index.get(entity_id)
    if previous is not None and previous is not row:
        logger.warning(
            "Duplicate CRM %s id %r in different aggregate rows; "
            "keeping the first indexed row",
            entity_type,
            entity_id,
        )
        return

    index[entity_id] = row


def _build_indexes(rows):
    by_customer_id = {}
    by_lead_id = {}
    by_customer_login = defaultdict(set)
    by_lead_login = defaultdict(set)

    for row in rows:
        for item in row.amo_customers or []:
            _add_unique_id(
                by_customer_id, _general_id(item), row, "customer"
            )
            login = _general_login(item)
            if login:
                by_customer_login[login].add(row)

        for item in row.amo_leads or []:
            _add_unique_id(by_lead_id, _general_id(item), row, "lead")
            login = _general_login(item)
            if login:
                by_lead_login[login].add(row)

    return (
        by_customer_id,
        by_lead_id,
        by_customer_login,
        by_lead_login,
    )

def _unique_login(index, login):
    if not login:
        return None

    matches = index.get(login)
    if not matches:
        return None

    if len(matches) != 1:
        logger.warning(
            "Ambiguous CRM login %r: %d aggregate rows",
            login,
            len(matches),
        )
        return None

    return next(iter(matches))



def _remove_index(index, key, row, field):
    if not key:
        return

    still_present = any(
        _general_login(item) == key
        for item in (getattr(row, field) or [])
    )
    if still_present:
        return

    matches = index.get(key)
    if matches:
        matches.discard(row)
        if not matches:
            index.pop(key, None)

def _add_index(index, key, row):
    if key:
        index[key].add(row)


def _remove_row_from_login_indexes(
    row,
    customer_login_index,
    lead_login_index,
):
    for items, index in (
        (row.amo_customers or [], customer_login_index),
        (row.amo_leads or [], lead_login_index),
    ):
        for login in {_general_login(item) for item in items} - {None, ""}:
            matches = index.get(login)
            if matches:
                matches.discard(row)
                if not matches:
                    index.pop(login, None)

def _empty_seen_bucket():
    return {field: set() for field in ENTITY_FIELDS}

def _mark_seen(seen, row, field, entity_ids):
    bucket = seen.setdefault(id(row), _empty_seen_bucket())
    bucket[field].update(entity_ids)

def _merge_rows(
    session,
    target,
    source,
    indexes,
    seen,
    rows_by_object_id,
):
    if target is source:
        return target

    for field in ENTITY_FIELDS:
        setattr(
            target,
            field,
            _merge_items(
                getattr(target, field),
                getattr(source, field),
            ),
        )

    (
        by_customer_id,
        by_lead_id,
        customer_login_index,
        lead_login_index,
    ) = indexes

    for mapping in (by_customer_id, by_lead_id):
        for entity_id, indexed_row in list(mapping.items()):
            if indexed_row is source:
                mapping[entity_id] = target

    _remove_row_from_login_indexes(
        source,
        customer_login_index,
        lead_login_index,
    )

    for item in target.amo_customers or []:
        _add_index(
            customer_login_index,
            _general_login(item),
            target,
        )

    for item in target.amo_leads or []:
        _add_index(
            lead_login_index,
            _general_login(item),
            target,
        )

    source_seen = seen.pop(id(source), None)
    if source_seen:
        target_seen = seen.setdefault(id(target), _empty_seen_bucket())
        for field in ENTITY_FIELDS:
            target_seen[field].update(source_seen[field])

    rows_by_object_id.pop(id(source), None)

    if inspect(source).pending:
        session.expunge(source)
    elif inspect(source).persistent:
        session.delete(source)

    return target

def _cleanup(session, rows, seen):
    for row in rows:
        keep = seen.get(id(row), _empty_seen_bucket())

        for field in ENTITY_FIELDS:
            old_items = getattr(row, field) or []
            filtered_items = [
                item
                for item in old_items
                if _general_id(item) in keep[field]
                and "удалить" not in _general_name(item).casefold()
            ]

            if filtered_items != old_items:
                setattr(row, field, filtered_items)

        if not (row.amo_customers or row.amo_leads):
            state = inspect(row)
            if state.pending:
                session.expunge(row)
            elif state.persistent:
                session.delete(row)

def _new_aggregate(session, rows_by_object_id, **kwargs):
    row = AbstractCustomer(**kwargs)
    session.add(row)
    rows_by_object_id[id(row)] = row
    return row

def collect_leads_customers(is_full=True):
    session = db.session

    try:
        rows = session.scalars(select(AbstractCustomer)).all()
        rows_by_object_id = {id(row): row for row in rows}

        indexes = _build_indexes(rows)
        (
            by_customer_id,
            by_lead_id,
            by_customer_login,
            by_lead_login,
        ) = indexes

        seen = {}
        processed = 0

        logger.info("Collecting CRM customers (full=%s)", is_full)
        customer_stream = amocrm_get(
            Customer,
            _with=f"{COMPANIES},{CONTACTS}",
        )

        for customer in customer_stream:
            if not is_full and customer.status_id == 111349:
                continue

            login = customer.get_field("Логин")
            if not login:
                continue

            # Customer id has priority over login matching.
            row = by_customer_id.get(customer.id)
            if row is None:
                row = _unique_login(by_lead_login, login)

            if row is None:
                row = _new_aggregate(
                    session,
                    rows_by_object_id,
                    amo_customers=[],
                )

            previous_customer = next(
                (
                    item
                    for item in (row.amo_customers or [])
                    if _general_id(item) == customer.id
                ),
                None,
            )
            previous_login = _general_login(previous_customer)

            customer_json = get_customer_json(customer)
            row.amo_customers = _merge_items(
                row.amo_customers,
                [customer_json],
            )
            row.amo_contacts = _merge_items(
                row.amo_contacts,
                _embedded_json(customer.contacts),
            )
            row.amo_companies = _merge_items(
                row.amo_companies,
                _embedded_json(customer.companies),
            )

            if previous_login and previous_login != login:
                _remove_index(
                    by_customer_login,
                    previous_login,
                    row,
                    "amo_customers",
                )

            _add_unique_id(
                by_customer_id,
                customer.id,
                row,
                "customer",
            )
            _add_index(by_customer_login, login, row)

            _mark_seen(seen, row, "amo_customers", [customer.id])

            _mark_seen(
                seen,
                row,
                "amo_contacts",
                [
                    item["id"]
                    for item in (customer.contacts or [])
                    if isinstance(item, dict) and item.get("id") is not None
                ],
            )
            _mark_seen(
                seen,
                row,
                "amo_companies",
                [
                    item["id"]
                    for item in (customer.companies or [])
                    if isinstance(item, dict) and item.get("id") is not None
                ],
            )

            processed += 1
            if processed % FLUSH_BATCH_SIZE == 0:
                session.flush()
            
        logger.info("Collecting CRM leads (full=%s)", is_full)
        lead_stream = amocrm_get(
            Lead,
            _with=f"{CONTACTS}",
        )

        for lead in lead_stream:
            if not is_full and lead.status_id == 143:
                continue

            login = lead.get_field("Логин")
            row_by_id = by_lead_id.get(lead.id)
            row_by_login = _unique_login(by_customer_login, login)

            if row_by_id is not None:
                # Lead-id match wins. If login identifies another aggregate,
                # merge that aggregate into the lead-id aggregate.
                row = row_by_id
                if row_by_login is not None and row_by_login is not row:
                    row = _merge_rows(
                        session,
                        target=row,
                        source=row_by_login,
                        indexes=indexes,
                        seen=seen,
                        rows_by_object_id=rows_by_object_id,
                    )
            else:
                row = row_by_login

            if row is None:
                row = _new_aggregate(
                    session,
                    rows_by_object_id,
                    amo_leads=[],
                )

            previous_lead = next(
                (
                    item
                    for item in (row.amo_leads or [])
                    if _general_id(item) == lead.id
                ),
                None,
            )
            previous_login = _general_login(previous_lead)

            row.amo_leads = _merge_items(
                row.amo_leads,
                [get_lead_json(lead)],
            )
            row.amo_contacts = _merge_items(
                row.amo_contacts,
                _embedded_json(lead.contacts),
            )

            if previous_login and previous_login != login:
                _remove_index(
                    by_lead_login,
                    previous_login,
                    row,
                    "amo_leads",
                )
            _add_unique_id(by_lead_id, lead.id, row, "lead")
            _add_index(by_lead_login, login, row)

            _mark_seen(seen, row, "amo_leads", [lead.id])
            _mark_seen(
                seen,
                row,
                "amo_contacts",
                [
                    item["id"]
                    for item in (lead.contacts or [])
                    if isinstance(item, dict) and item.get("id") is not None
                ],
            )

            processed += 1
            if processed % FLUSH_BATCH_SIZE == 0:
                session.flush()

        if is_full:
            _cleanup(session, rows_by_object_id.values(), seen)

        session.commit()
        logger.info("CRM synchronization completed (full=%s)", is_full)

    except Exception:
        session.rollback()
        logger.exception(
            "CRM synchronization failed; transaction rolled back and "
            "destructive cleanup was not committed"
        )
        raise