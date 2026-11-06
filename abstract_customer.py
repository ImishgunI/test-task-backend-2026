import json
import logging
from collections import defaultdict
from typing import Optional, Dict

from sqlalchemy import text

from amocrm import Customer, Lead
from models import AbstractCustomer, db, AbcCustomerEntities

logger = logging.getLogger(__name__)


def dict_compare_merge(left, right):
    merged = {}
    for item in (left or []) + (right or []):
        general = item.get("general") or {}
        entity_id = general.get("id")
        if entity_id is None:
            continue
        current = merged.setdefault(entity_id, {})
        current.update(item)
        current_general = dict(current.get("general") or {})
        current_general.update(general)
        current["general"] = current_general
    return list(merged.values())

def _field(entity, name):
    values = entity.custom_fields_values or {}
    value = values.get(name) or {}
    return value.get("value")

def get_customer_json(customer: Customer) -> dict:
    return {
        "general": {
            "id": customer.id,
            "login": _field(customer, "Логин"),
            "billing_type": _field(customer, "Вид оплаты"),
            "is_robot": bool(customer.custom_fields_values and customer.custom_fields_values.get("Робот")),
            "is_nds": bool(customer.custom_fields_values and customer.custom_fields_values.get("Оплата с НДС")),
            "system": _field(customer, "Система"),
            "host": _field(customer, "Хост"),
            "name": customer.name,
        },
        "api_value": customer.as_dict(),
    }


def get_lead_json(lead: Lead) -> dict:
    return {
        "general": {
            "id": lead.id,
            "login": _field(lead, "Логин"),
            "name": lead.name,
        },
        "api_value": lead.as_dict(),
    }


def get_abccustomer_by_lead_login(lead_login: str) -> Optional[AbstractCustomer]:
    if not lead_login:
        return None
    matches = (AbstractCustomer.query
               .filter(AbstractCustomer.amo_leads.op("@>")(
                   [{"general": {"login": lead_login}}]))
               .limit(2).all())
    if len(matches) > 1:
        logger.warning("Lead login %r is ambiguous", lead_login)
        return None
    return matches[0] if matches else None


def get_abccustomer_by_amocustomer_login(customer_login: str) -> Optional[AbstractCustomer]:
    if not customer_login:
        return None
    matches = (AbstractCustomer.query
               .filter(AbstractCustomer.amo_customers.op("@>")(
                   [{"general": {"login": customer_login}}]))
               .limit(2).all())
    if len(matches) > 1:
        logger.warning("Customer login %r is ambiguous", customer_login)
        return None
    return matches[0] if matches else None


def create_or_update_customer(abstract_customer: Optional[AbstractCustomer],
                              amo_customer: Customer) -> AbstractCustomer:
    value = get_customer_json(amo_customer)
    if abstract_customer is None:
        abstract_customer = AbstractCustomer(amo_customers=[value])
        from models import db
        db.session.add(abstract_customer)
    else:
        abstract_customer.amo_customers = dict_compare_merge(
            abstract_customer.amo_customers, [value])
    return abstract_customer


def create_or_update_lead(abc_customer: Optional[AbstractCustomer],
                          amo_lead: Lead) -> AbstractCustomer:
    value = get_lead_json(amo_lead)
    if abc_customer is None:
        abc_customer = AbstractCustomer(amo_leads=[value])
        from models import db
        db.session.add(abc_customer)
    else:
        abc_customer.amo_leads = dict_compare_merge(abc_customer.amo_leads, [value])
    return abc_customer
