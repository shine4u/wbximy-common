from unittest import TestCase
from wbximy_common.gslib.credit_code import credit_code_incr, credit_code_valid
from wbximy_common.libs.log import setup_logger

logger = setup_logger()


class TestCreditCode(TestCase):

    def test_1(self):
        self.assertTrue(credit_code_incr('53640000MJX173672F'))
        a = credit_code_incr('53640000MJX173672F', 1)
        self.assertEqual(a, '53640000MJX173680A')

