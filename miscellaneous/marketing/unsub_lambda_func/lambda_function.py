"""Sync unsubscription events between Braze and Marketo."""
import json
import logging
import os

import requests
from marketorestpython.client import MarketoClient
from marketorestpython.helper.exceptions import MarketoException

logging.basicConfig()

# Create a custom logger
logger = logging.getLogger(__name__)

# Create handlers
c_handler = logging.StreamHandler()

# Create formatters and add it to handlers
c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)

# Add handlers to the ogger
logger.addHandler(c_handler)
logger.setLevel(logging.DEBUG)


BRAZE_SOURCE = "braze"
MARKETO_SOURCE = "marketo"
HTTP_SUCCESS_STATUS_RANGE = (200, 299)


class BrazeAPIError(Exception):
    """Throw this exception when failed to call Braze API."""

    def __init__(self, message, errors):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

        # Now for your custom code...
        self.errors = errors


class MarketoUnsubscribeHandler:
    """Handle unsubscribe events on Marketo."""

    def __init__(self, client_id, client_secret, munchkin_id):
        api_limit = None
        max_retry_time = None
        requests_timeout = (3.0, 10.0)
        self.mc = MarketoClient(
            munchkin_id,
            client_id,
            client_secret,
            api_limit,
            max_retry_time,
            requests_timeout=requests_timeout,
        )

    def unsubsribe(self, email):
        """Unsubscribe by email."""
        leads = self._get_leads([email])

        logger.debug("found %d leads for input email", len(leads))

        unsubcribe_leads = []
        for lead in leads:
            if lead["email"] != email:
                logger.debug(
                    "Found different email, lead_email=%s, input_email=%s",
                    lead["email"],
                    email,
                )
                continue

            if lead["unsubscribed"]:
                logger.debug("Lead is already unsubscribed, lead_id=%s", lead["id"])
                continue

            lead["unsubscribed"] = "true"
            lead[
                "unsubscribedReason"
            ] = "Cross instance sync triggered by webhook from braze"

            unsubcribe_leads.append(lead)

        if not unsubcribe_leads:
            return 0

        updated_leads = self.mc.execute(
            method="create_update_leads",
            leads=unsubcribe_leads,
            action="updateOnly",
            lookupField="id",
            asyncProcessing="true",
            partitionName="Default",
        )

        if updated_leads:
            logger.debug(
                "unsubscribed leads, lead_ids=%s",
                [lead["id"] for lead in updated_leads],
            )
            return len(updated_leads)
        return 0

    def _get_leads(self, emails: list[str]):
        return self.mc.execute(
            method="get_multiple_leads_by_filter_type",
            filterType="email",
            filterValues=emails,
            fields=["firstName", "id", "unsubscribed"],
            batchSize=None,
        )


class BrazeUnsubscribeHandler:
    """Handle unsubscribe events on Braze."""

    def __init__(self, api_key, braze_endpoint):
        self.api_key = api_key
        self.endpoint = braze_endpoint

    def unsubscribe(self, email):
        """Unsubscribe by email."""
        external_id_with_emails = self.get_externalids(email)

        external_id_to_unsubscribe = [
            el["external_id"]
            for el in external_id_with_emails
            if email == el["email"] and el["email_subscribe"] == "subscribed"
        ]

        if len(external_id_to_unsubscribe) <= 0:
            return 0

        resp = requests.post(
            self.get_url("users/track"),
            json={
                "attributes": [
                    {
                        "external_id": exid,
                        "email": email,
                        "email_subscribe": "unsubscribed",
                    }
                    for exid in external_id_to_unsubscribe
                ]
            },
            headers=self.get_headers(),
            timeout=300,
        )

        self.raise_on_failed(resp)

        resp_body = json.loads(resp.text)
        return resp_body.get("attributes_processed", 0)

    def raise_on_failed(self, resp):
        """Raise exception when http status not in [200, 300)."""
        if (
            resp.status_code < HTTP_SUCCESS_STATUS_RANGE[0]
            or resp.status_code > HTTP_SUCCESS_STATUS_RANGE[1]
        ):
            raise BrazeAPIError(resp.text, None)

    def get_externalids(self, email):
        """Get external ids by email."""
        resp = requests.post(
            self.get_url("/users/export/ids"),
            json={
                "email_address": email,
                "fields_to_export": ["external_id", "email", "email_subscribe"],
            },
            headers=self.get_headers(),
            timeout=300,
        )

        self.raise_on_failed(resp)
        return json.loads(resp.text)["users"]

    def get_headers(self):
        """Get Braze API headers."""
        return {
            "Authorization": f"Bearer {self.api_key}",  # Example authorization header
            "Content-Type": "application/json",  # Example content type
        }

    def get_url(self, path):
        """Format url."""
        return f"{self.endpoint}/{path}"


def lambda_handler(event, _context):
    """Endpoint."""
    logger.debug("Handling unsub event: %s", event)

    rq_body: dict[str, str] = json.loads(event["body"]) if "body" in event else event

    email = rq_body.get("email", None)
    if not email:
        return {"statusCode": 400, "body": json.dumps("Missing email")}

    source = rq_body.get("source", None)
    if source == BRAZE_SOURCE:
        handler = MarketoUnsubscribeHandler(
            os.environ["M_CLIENT_ID"],
            os.environ["M_CLIENT_SECRET"],
            os.environ["M_MUNCHKIN_ID"],
        )

        unsubscribed_leads_cnt = 0
        try:
            unsubscribed_leads_cnt = handler.unsubsribe(email)
        except MarketoException as e:
            return {"statusCode": 500, "body": json.dumps(f"Error: {e}")}

        return {
            "statusCode": 200,
            "body": json.dumps(f"Unsubcried {unsubscribed_leads_cnt} users in Marketo"),
        }

    if source == MARKETO_SOURCE:
        handler = BrazeUnsubscribeHandler(
            os.environ["BRAZE_API_KEY"], "https://rest.iad-05.braze.com"
        )
        try:
            processed = handler.unsubscribe(email)
        except BrazeAPIError as e:
            return {"statusCode": 500, "body": json.dumps(f"Error: {e}")}

        return {
            "statusCode": 200,
            "body": json.dumps(f"Unsubcried {processed} users in Braze."),
        }
    return {"statusCode": 400, "body": json.dumps("Unknow source")}
