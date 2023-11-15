from __future__ import annotations
from enum import Enum
from pydantic import BaseModel


class OrganizationState(BaseModel):
    active: bool
    status: OrganizationStatus
    status_detail: OrganizationStatusDetail | None = None

    @staticmethod
    def from_uk_data(status, status_detail):
        if status_detail is not None:
            return OrganizationState(active=status not in ['DISSOLVED', 'CONVERTED_CLOSED'], status=OrganizationStatus(status),
                                     status_detail=OrganizationStatusDetail(status_detail))
        else:
            return OrganizationState(active=status not in ['DISSOLVED', 'CONVERTED_CLOSED'], status=OrganizationStatus(status))

    @staticmethod
    def from_deu_data(status):
        return OrganizationState(active=status not in ['LIQUIDATED', 'INACTIVE'], status=OrganizationStatus(status))


class OrganizationStatus(Enum):
    unknown: str = "UNKNOWN"
    in_active: str = "INACTIVE"
    active: str = "ACTIVE"
    liquidated: str = "LIQUIDATED"

    dissolved: str = "DISSOLVED"
    liquidation: str = "LIQUIDATION"
    receivership: str = "RECEIVER_ACTION"
    converted_closed: str = "CONVERTED_CLOSED"
    voluntary_arrangement: str = "VOLUNTARY_ARRANGEMENT"
    insolvency_proceedings: str = "INSOLVENCY_PROCEEDINGS"
    in_administration: str = "IN_ADMINISTRATION"
    closed: str = "CLOSED"
    open: str = "OPEN"
    registered: str = "REGISTERED"


class OrganizationStatusDetail(Enum):
    active_proposal_to_strike_off: str = "ACTIVE_PROPOSAL_TO_STRIKE_OFF"
    converted_to_plc: str = "CONVERTED_TO_PLC"
    converted_to_uk_societas: str = "CONVERTED_TO_UK_SOCIETAS"
    converted_to_ukeig: str = "CONVERTED_TO_UKEIG"
    transfer_from_uk: str = "TRANSFER_FROM_UK"
    transformed_to_se = "TRANSFORMED_TO_SE"
