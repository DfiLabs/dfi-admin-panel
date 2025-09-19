import os
import datetime
import boto3
from botocore.exceptions import ClientError


class EmailNotifier:
    """Send SES emails with daily throttle to avoid duplicates."""

    def __init__(self,
                 region: str = "eu-west-1",
                 sender: str = "hello@dfi-labs.com",
                 recipient: str = "hello@dfi-labs.com",
                 throttle_state_path: str = "/home/ubuntu/.signal_dashboard_last_email") -> None:
        self.region = region
        self.sender = sender
        self.recipient = recipient
        self.throttle_state_path = throttle_state_path
        self.client = boto3.client("sesv2", region_name=region)

    def _already_sent_today(self) -> bool:
        try:
            if not os.path.exists(self.throttle_state_path):
                return False
            with open(self.throttle_state_path, "r") as f:
                last = f.read().strip()
            return last == datetime.date.today().isoformat()
        except Exception:
            return False

    def _mark_sent_today(self) -> None:
        try:
            with open(self.throttle_state_path, "w") as f:
                f.write(datetime.date.today().isoformat())
        except Exception:
            # Best-effort; do not raise
            pass

    def send_once_per_day(self, subject: str, html_body: str, text_body: str | None = None) -> bool:
        """Send an email if not already sent today. Returns True on success."""
        if self._already_sent_today():
            return False

        try:
            content = {
                "Simple": {
                    "Subject": {"Data": subject, "Charset": "UTF-8"},
                    "Body": {
                        "Html": {"Data": html_body, "Charset": "UTF-8"},
                    },
                }
            }
            if text_body:
                content["Simple"]["Body"]["Text"] = {"Data": text_body, "Charset": "UTF-8"}

            self.client.send_email(
                FromEmailAddress=self.sender,
                Destination={"ToAddresses": [self.recipient]},
                Content=content,
            )
            self._mark_sent_today()
            return True
        except ClientError as e:
            print(f"[EmailNotifier] SES send_email failed: {e}")
            return False


