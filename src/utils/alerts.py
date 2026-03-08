"""
Configurable alerts — sends notifications to Slack webhook and/or email when conditions are met.
"""

from __future__ import annotations

import logging
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

logger = logging.getLogger(__name__)

_last_arbitrage_alert: dict[str, datetime] = {}
_last_volume_alert: dict[str, datetime] = {}
_alert_cooldown_sec = 60  # Don't re-alert same condition within 60 seconds


def _send_slack(webhook_url: str, text: str, blocks: list[dict] | None = None) -> bool:
    """Send a message to Slack via webhook. Returns True on success."""
    if not webhook_url or not webhook_url.startswith("https://"):
        return False
    try:
        payload = {"text": text}
        if blocks:
            payload["blocks"] = blocks
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code != 200:
            logger.warning("Slack webhook failed: %s %s", resp.status_code, resp.text[:200])
            return False
        return True
    except Exception as e:
        logger.warning("Slack webhook error: %s", e)
        return False


def _send_email(
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    smtp_use_tls: bool,
    to_addresses: list[str],
    subject: str,
    body: str,
) -> bool:
    """Send an email via SMTP. Returns True on success."""
    if not smtp_host or not to_addresses:
        return False
    try:
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = smtp_user or "crypto-alerts@localhost"
        msg["To"] = ", ".join(to_addresses)
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            if smtp_use_tls:
                server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.sendmail(msg["From"], to_addresses, msg.as_string())
        return True
    except Exception as e:
        logger.warning("Email send error: %s", e)
        return False


def alert_arbitrage(
    webhook_url: str,
    product_id: str,
    cheap_exchange: str,
    expensive_exchange: str,
    cheap_price: float,
    expensive_price: float,
    spread_pct: float,
    *,
    email_to: str = "",
    smtp_host: str = "",
    smtp_port: int = 587,
    smtp_user: str = "",
    smtp_password: str = "",
    smtp_use_tls: bool = True,
) -> bool:
    """Send arbitrage opportunity alert to Slack and/or email."""
    key = f"{product_id}:{cheap_exchange}:{expensive_exchange}"
    now = datetime.now(timezone.utc)
    if key in _last_arbitrage_alert:
        if (now - _last_arbitrage_alert[key]).total_seconds() < _alert_cooldown_sec:
            return False
    _last_arbitrage_alert[key] = now

    text = (
        f"Arbitrage opportunity: {product_id}\n"
        f"- Buy on {cheap_exchange} @ ${cheap_price:,.2f}\n"
        f"- Sell on {expensive_exchange} @ ${expensive_price:,.2f}\n"
        f"- Spread: {spread_pct:.2f}%"
    )
    slack_ok = _send_slack(webhook_url, text)
    email_ok = False
    if email_to and smtp_host:
        recipients = [a.strip() for a in email_to.split(",") if a.strip()]
        email_ok = _send_email(
            smtp_host, smtp_port, smtp_user, smtp_password, smtp_use_tls,
            recipients,
            subject=f"[Crypto Alert] Arbitrage: {product_id} {spread_pct:.2f}%",
            body=text,
        )
    if slack_ok or email_ok:
        logger.info("Arbitrage alert sent: %s %.2f%%", product_id, spread_pct)
    return slack_ok or email_ok


def alert_volume_spike(
    webhook_url: str,
    product_id: str,
    current_volume: float,
    baseline_volume: float,
    spike_ratio: float,
    *,
    email_to: str = "",
    smtp_host: str = "",
    smtp_port: int = 587,
    smtp_user: str = "",
    smtp_password: str = "",
    smtp_use_tls: bool = True,
) -> bool:
    """Send volume spike alert to Slack and/or email."""
    key = product_id
    now = datetime.now(timezone.utc)
    if key in _last_volume_alert:
        if (now - _last_volume_alert[key]).total_seconds() < _alert_cooldown_sec:
            return False
    _last_volume_alert[key] = now

    text = (
        f"Volume spike: {product_id}\n"
        f"- Current: {current_volume:.4f}\n"
        f"- Baseline: {baseline_volume:.4f}\n"
        f"- Ratio: {spike_ratio:.1f}x"
    )
    slack_ok = _send_slack(webhook_url, text)
    email_ok = False
    if email_to and smtp_host:
        recipients = [a.strip() for a in email_to.split(",") if a.strip()]
        email_ok = _send_email(
            smtp_host, smtp_port, smtp_user, smtp_password, smtp_use_tls,
            recipients,
            subject=f"[Crypto Alert] Volume spike: {product_id} {spike_ratio:.1f}x",
            body=text,
        )
    if slack_ok or email_ok:
        logger.info("Volume spike alert sent: %s %.1fx", product_id, spike_ratio)
    return slack_ok or email_ok
