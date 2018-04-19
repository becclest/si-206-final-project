import unittest
from finalproject import *


class TestDatabase(unittest.TestCase):
    def test_international_table(self):
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        sql = '''
            SELECT Headline
            FROM International
            WHERE Alpha2="SY"
        '''
        results = cur.execute(sql)
        result_list = results.fetchall()
        self.assertIn(('Trump',), result_list)
        self.assertEqual(len(result_list), 224)

        sql = '''
            SELECT COUNT(*)
            FROM International
        '''
        results = cur.execute(sql)
        count = results.fetchone()[0]
        self.assertEqual(count, 998)

        conn.close()

    def test_domestic_table(self):
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        sql = '''
            SELECT Headline
            FROM Domestic
            WHERE Polarity=0.0
        '''
        results = cur.execute(sql)
        result_list = results.fetchall()
        self.assertIn(('Assad',), result_list)
        self.assertEqual(len(result_list), 136)

        sql = '''
            SELECT COUNT(*)
            FROM Domestic
        '''
        results = cur.execute(sql)
        count = results.fetchone()[0]
        self.assertEqual(count, 425)

        conn.close()

unittest.main(verbosity=4)
