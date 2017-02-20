"""
email.py - various functions to work with emails in the pdata application
"""
import argparse
import logging
from smtplib import SMTPException
import sys

import django
django.setup()
from django.core.mail import EmailMessage

from pdata_app.models import EmailQueue

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


EMAIL_HOST = 'exchsmtp.stfc.ac.uk'
EMAIL_PORT = 587
EMAIL_USE_TLS = True

FROM_ADDRESS = 'no-reply@prima-dm.ceda.ac.uk'
REPLY_TO_ADDRESS = 'jon.seddon@metoffice.gov.uk'


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Send any unsent emails that '
                                                 'are in the queue.')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
        'debug, info, warn (the default), or error')
    args = parser.parse_args()

    return args


def main():
    """
    Send any queued emails

    :return:
    """
    for email in EmailQueue.objects.filter(sent=False):
        if not email.recipient.email:
            msg = 'No email address is available for user {}'.format(
                email.recipient.username)
            logger.warning(msg)
        else:
            email_msg = EmailMessage(
                subject=email.subject,
                body=email.message,
                from_email=FROM_ADDRESS,
                reply_to=REPLY_TO_ADDRESS,
                to=email.recipient.email
            )

            try:
                email_msg.send()
            except SMTPException as exc:
                msg = 'Unable to send email to {} with subject {}.\n{}'.format(
                    email.recipient.email, email.subject, exc.message)
                logger.warning(msg)
            else:
                email.sent = True
                email.save()


if __name__ == '__main__':
    cmd_args = parse_args()

    # Disable propagation and discard any existing handlers.
    logger.propagate = False
    if len(logger.handlers):
        logger.handlers = []

    # set-up the logger
    console = logging.StreamHandler(stream=sys.stdout)
    fmtr = logging.Formatter(fmt=DEFAULT_LOG_FORMAT)
    if cmd_args.log_level:
        try:
            logger.setLevel(getattr(logging, cmd_args.log_level.upper()))
        except AttributeError:
            logger.setLevel(logging.WARNING)
            logger.error('log-level must be one of: debug, info, warn or error')
            sys.exit(1)
    else:
        logger.setLevel(DEFAULT_LOG_LEVEL)
    console.setFormatter(fmtr)
    logger.addHandler(console)

    # run the code
    main()
