import logging
log = logging.getLogger(__name__)

import time

from . import constants


class OgUpdateLinks(object):

    def get_update_batch( self, txn, queue ):
        """selects database rows for update, creates an application specific lock."""
        log.debug("OgUpdateLinks[%s].get_update_batch", id(self))
        time.sleep(2)
        return range(1,20)


    def release_all_locked( self, txn ):
        """releases database rows selected for update via `get_update_batch`"""
        log.debug('OgUpdateLinks[%s].release_all_locked()', id(self) )
        time.sleep(2)
        return True


    def action_for_data( self, txn, data=None ):
        """parses link for metadata"""
        log.debug('OgUpdateLinks[%s].action_for_data(%s)', id(self), data )
        time.sleep(5)
        return True

