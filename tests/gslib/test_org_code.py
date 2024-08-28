from unittest import TestCase
from wbximy_common.gslib.org_code import org_code_valid, org_code_incr
from wbximy_common.libs.log import setup_logger

logger = setup_logger()


class TestOrgCode(TestCase):

    def test_1(self):
        self.assertTrue(org_code_valid('MJX173699'))
        a = org_code_incr('MJX173699', 1, only_numbers=True)
        self.assertEqual(a, 'MJX173705')

