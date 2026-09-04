#===============================================================================
from unittest import TestCase

from src.models import Order, OrderCreate

from tests.helpers import make_account, make_symbol, make_tick, time_after

################################################################################
#===============================================================================
class TestOrderDelete(TestCase):
    #===============================================================================
    def setUp(self):
        self.symbol = make_symbol()
        self.account = make_account(self.symbol)
        self.quote = make_tick(self.symbol)
        request = OrderCreate(account = self.account, symbol = self.symbol,
            size = 1, price = 98.0)
        self.order = Order.from_request(request, self.quote)
        self.assertIsInstance(self.order, Order)

    #===============================================================================
    def test_delete_request_keeps_order_identity(self):
        """[C] A placed order is available for cancellation.
        [S] Generate an OrderDelete request from that order.
        [D] The request keeps the order UID and uses the supplied quote time.
        [N] Deletion does not itself mutate the order book.
        """
        delete = self.order.delete(self.quote)

        self.assertEqual(delete.UID, self.order.UID)
        self.assertEqual(delete.time, self.quote.time_event)
        self.assertEqual(delete.account, self.account)

    #===============================================================================
    def test_order_delete_marks_order_dumped(self):
        """[C] A placed order receives a valid delete request.
        [S] Apply the request to the order instance.
        [D] The order becomes DUMPED and records the deletion timestamp.
        [N] A later timestamp makes the state transition observable.
        """
        delete = self.order.delete(make_tick(self.symbol, time = time_after(1)))

        self.order.on_delete(delete)

        self.assertEqual(self.order.status, Order.Status.DUMPED)
        self.assertEqual(self.order.time, delete.time)
