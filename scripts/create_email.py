#!/usr/bin/env python
"""
create_email.py - compose an email and add it to the queue of emails to be
                  sent in the database.
"""
from __future__ import unicode_literals, division, absolute_import
import argparse
import logging.config
from smtplib import SMTPException
import sys

import django
django.setup()

from django.contrib.auth.models import User
from pdata_app.models import EmailQueue

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)
TO_USERNAME = 'jseddon'


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Compose an email and queue'
                                                 'it for delivery.')
    parser.add_argument('subject', help="The email's subject")
    parser.add_argument('body', help="The body of the email")
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
                                                  'debug, info, warn (the '
                                                  'default), or error')
    args = parser.parse_args()

    return args


def main(args):
    """
    Send any queued emails

    :return:
    """
    try:
        recipient = User.objects.get(username=TO_USERNAME)
    except django.core.exceptions.ObjectDoesNotExist:
        logger.error('Username {} cannot be found. Email not queued.')
        sys.exit(1)
    logger.debug('To: {}'.format(recipient.email))
    logger.debug('Subject: {}'.format(args.subject))
    logger.debug('Message: {}'.format(args.body))
    EmailQueue.objects.create(recipient=recipient, subject=args.subject,
                              message=args.body)


if __name__ == '__main__':
    cmd_args = parse_args()

    # determine the log level
    if cmd_args.log_level:
        try:
            log_level = getattr(logging, cmd_args.log_level.upper())
        except AttributeError:
            logger.setLevel(logging.WARNING)
            logger.error('log-level must be one of: debug, info, warn or error')
            sys.exit(1)
    else:
        log_level = DEFAULT_LOG_LEVEL

    # configure the logger
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': DEFAULT_LOG_FORMAT,
            },
        },
        'handlers': {
            'default': {
                'level': log_level,
                'class': 'logging.StreamHandler',
                'formatter': 'standard'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': log_level,
                'propagate': True
            }
        }
    })

    # run the code
    main(cmd_args)
