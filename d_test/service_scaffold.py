import logging
log = logging.getLogger(__name__)

from twisted.application import internet
from twisted.python import threadable
from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet import task
from twisted.internet import threads
from twisted.enterprise import adbapi
from twisted.enterprise.adbapi import ConnectionPool


from . import constants
from . import database



class RequestWrapper():
    deferredSemaphoreService = None
    dbConnectionPool = None


    def __init__( self , deferredSemaphoreService = None , dbConnectionPool = None ):
        log.debug("%s[%s].__init__" , self.__class__.__name__ , id(self) )
        self.dbConnectionPool = dbConnectionPool
        self.deferredSemaphoreService = deferredSemaphoreService


    def queue_thread( self , data=None ):
        """queues an item into a thread.
        
            uses a DeferredSemaphore(_ConcurrencyPrimitive)
                https://twistedmatrix.com/documents/current/api/twisted.internet.defer.DeferredSemaphore.html
                
            calls via `run` <_ConcurrencyPrimitive>
                https://twistedmatrix.com/documents/current/api/twisted.internet.defer._ConcurrencyPrimitive.html#run
                
            ===========
                
            > def run(*args, **kwargs): (source)
            >     Acquire, run, release.
            >    This function takes a callable as its first argument and any number of other positional and keyword arguments. When the lock or semaphore is acquired, the callable will be invoked with those arguments.
            > 
            >     The callable may return a `Deferred`; if it does, the lock or semaphore won't be released until that Deferred fires.

            >     Returns	`Deferred` of function result.

            ===========
        """
        log.debug("%s[%s].queue_thread" , self.__class__.__name__ , id(self) )
        self.queued_data = data
        
        if constants.USE_BROKEN :
            d = self.deferredSemaphoreService.run( self._T_to_thread )
        else:
            d = self.deferredSemaphoreService.run( self._T_thread_begin )
        log.debug("%s[%s].queue_thread || FIN | %s" , self.__class__.__name__ , id(self) , d )
        if constants.RETURN_D__QUEUE_THREAD :
            return d


    def _T_to_thread( self ):
        log.debug("%s[%s]._T_to_thread" , self.__class__.__name__ , id(self) )
        d = threads.deferToThread( self._T_thread_begin )\
            .addErrback( self._T_errors )
        log.debug("%s[%s]._T_to_thread FIN = %s" , self.__class__.__name__ , id(self) , d )
        if constants.RETURN_D__TO_THREAD :
            return d


    def _T_thread_begin( self ):
        """This should be overridden"""
        raise ValueError("""OVERRIDE ME""")
        if constants.RETURN_D__THREAD_BEGIN :
            return d

	
    def _T_errors( self , x ):
        log.debug("%s[%s]._T_errors || %s" , self.__class__.__name__, id(self) , x )
        raise x




class ServiceScaffold():
    _inited = None
    _is_processing_flag = False
    
    # defer.Deferred()
    d = None

    # defer.DeferredList()
    d_list = None

    importer = None
    updater = None

    deferredSemaphoreService = None
    SEMAPHORE_TOKENS = 10


    def __init__(self):
        """create a new service. only need one per application"""
        log.debug("%s[%s].__init__ (ServiceScaffold)" , self.__class__.__name__ , id(self) )
        self.deferredSemaphoreService = defer.DeferredSemaphore( tokens=self.SEMAPHORE_TOKENS )


    def init_ensured( self , rval=None ):
        log.debug("%s[%s].init_ensured (ServiceScaffold)" , self.__class__.__name__  , id(self) )
        d = database.get_dbPool()\
            .runInteraction( self.updater.release_all_locked )\
            .addCallback( self.init_ensured_followup )\
            .addErrback( self.errback )
        self.d = d


    def init_ensured_followup( self , rval=None ):
        log.debug("%s[%s].init_ensured_followup (ServiceScaffold)" , self.__class__.__name__  , id(self) )
        self._inited = True
        self.d\
            .addCallback( self.get_batch )\
            .addErrback( self.errback )


    def get_is_processing( self ):
        if self._is_processing_flag is True:
            return True
        return False


    def set_processing_status( self , flag ):
        if flag not in ( True , False ):
            raise ValueError("invalid flag")
        self._is_processing_flag = flag


    def deferred_list_finish( self , d ):
        log.debug("%s[%s].deferred_list_finish (ServiceScaffold)" , self.__class__.__name__  , id(self) )
        self.set_processing_status( False )


    def service_start( self ):
        log.debug("%s[%s].service_start (ServiceScaffold)" , self.__class__.__name__  , id(self))

        if self._inited is not True:
            log.debug("%s[%s].service_start (ServiceScaffold)-  pre-init" , self.__class__.__name__  , id(self))
            self.d = defer.Deferred()
            self.d.addCallback( self.init_ensured )
            self.d.addErrback(self.errback)
            return self.d.callback(None)
        log.debug("%s[%s].service_start (ServiceScaffold)-  post-init" , self.__class__.__name__  , id(self))

        if self.get_is_processing() :
            log.debug("%s[%s].service_start (ServiceScaffold)-  still processing..." , self.__class__.__name__  , id(self))
            # return , no callbacks.  this kills it.
            return

        self.set_processing_status( True )
        self.d.addCallback( self.get_batch )


    def get_batch( self , rval=None ):
        log.debug("%s[%s].get_batch (ServiceScaffold)" , self.__class__.__name__  , id(self))
        raise ValueError("Overwrite Me!")


    def process_batch( self , rval=None ):
        log.debug("%s[%s].process_batch (ServiceScaffold)" , self.__class__.__name__  , id(self))
        raise ValueError("Overwrite Me!")


    def _action_error( self , raised ):
        """Special handling errback"""
        log.debug("%s[%s]._action_error (ServiceScaffold) | raised : %s" , self.__class__.__name__  , id(self))
        self.set_processing_status( False )


    def errback( self , raised ):
        """Generic rback, just prints the raised exception"""
        log.debug("%s[%s].errback (ServiceScaffold)" , self.__class__.__name__  , id(self))
        print raised
        print raised.__dict__

