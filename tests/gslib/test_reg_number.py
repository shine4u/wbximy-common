from unittest import TestCase
from wbximy_common.gslib.reg_number import reg_number_valid, reg_number_incr
from wbximy_common.libs.log import setup_logger

logger = setup_logger()


class TestRegNumber(TestCase):

    def test_1(self):
        self.assertTrue(reg_number_valid('14010700A001338'))
        a = reg_number_incr('14010700A001338', 1)
        self.assertEqual(a, '14010700A001339')

    def test_2(self):
        self.assertTrue(reg_number_valid('410000100015088'))
        a = reg_number_incr('410000100015088', 1)
        self.assertEqual(a, '410000100015096')
