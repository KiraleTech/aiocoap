import asyncio
import ipaddress
import urllib.parse

from kitools import kiserial

from .. import error, interfaces
from ..message import Message
from ..numbers import COAP_PORT
from ..util import hostportjoin, socknumbers


class KBIEndpointAddress(interfaces.EndpointAddress):
    def __init__(self, sockaddr):
        self.sockaddr = sockaddr

    def __hash__(self):
        return hash(self.sockaddr)

    def __eq__(self, other):
        return self.sockaddr == other.sockaddr

    def __repr__(self):
        return "<%s [%s]:%d>" % (
            type(self).__name__,
            self.sockaddr[0],
            self.sockaddr[1],
        )

    @property
    def hostinfo(self):
        port = self.sockaddr[1]
        if port == COAP_PORT:
            port = None

        return hostportjoin(self.sockaddr[0], port)

    @property
    def uri(self):
        return 'coap://' + self.hostinfo

    @property
    def is_multicast(self):
        try:
            result = ipaddress.IPv6Address(self.sockaddr[0]).is_multicast
            return result
        except:
            return False

    @property
    def is_multicast_locally(self):
        return self.is_multicast


class TransportEndpointKBI(asyncio.DatagramProtocol, interfaces.TransportEndpoint):
    def __init__(self, new_message_callback, new_error_callback, log, loop):
        self.new_message_callback = new_message_callback
        self.new_error_callback = new_error_callback
        self.log = log
        self.loop = loop

        self.kidev = None
        self.socket = None

    @classmethod
    @asyncio.coroutine
    def create_transport_endpoint(
        cls, kidev, port, new_message_callback, new_error_callback, log, loop
    ):
        protocol = cls(
            new_message_callback=new_message_callback,
            new_error_callback=new_error_callback,
            log=log,
            loop=loop,
        )

        protocol.kidev = kidev
        protocol.src_port = yield from protocol.kidev.socket_open(
            port, protocol.datagram_received
        )
        # TODO: don't continue if src_port is None
        return protocol

    async def shutdown(self):
        self.kidev.socket_close(self.src_port)
        del self.new_message_callback
        del self.new_error_callback

    def send(self, message):
        coro = self.kidev.socket_send(
            self.src_port,
            message.remote.sockaddr[1],
            message.remote.sockaddr[0],
            message.encode(),
        )
        asyncio.ensure_future(coro)

    async def determine_remote(self, request):
        if request.requested_scheme not in ('coap', None):
            return None

        if request.unresolved_remote is not None:
            pseudoparsed = urllib.parse.SplitResult(
                None, request.unresolved_remote, None, None, None
            )
            host = pseudoparsed.hostname
            port = pseudoparsed.port or COAP_PORT
        elif request.opt.uri_host:
            host = request.opt.uri_host
            port = request.opt.uri_port or COAP_PORT
        else:
            raise ValueError(
                "No location found to send message to (neither in .opt.uri_host nor in .remote)"
            )

        return KBIEndpointAddress((host, port))

    def connection_made(self, transport):
        pass

    def datagram_received(self, data, addr):
        try:
            message = Message.decode(data, KBIEndpointAddress(addr))
        except error.UnparsableMessage:
            self.log.warning("Ignoring unparsable message from %s" % addr)
            return

        self.new_message_callback(message)

    def datagram_errqueue_received(self, data, ancdata, flags, address):
        assert flags == socknumbers.MSG_ERRQUEUE
        errno = None

        remote = KBIEndpointAddress(address)

        self.new_error_callback(errno, remote)

    def error_received(self, exc):
        self.log.error("Error received and ignored in this codepath: %s" % exc)

    def connection_lost(self, exc):
        if exc is not None:
            self.log.error("Connection lost: %s" % exc)
