# -*- coding: utf-8 -*-
"""
Module Name: test_newt.py

Description:
This module contains the unit tests for the NEWT module. The unit tests in this 
module ensure the correctness and reliability of the functions utilised by using
mock objects to simulate database interactions and other dependencies.

Author: elreysausage
Date: 2024-11-15
"""

import unittest
from unittest.mock import patch, MagicMock

import pandas as pd

from strategies.newt import NEWTStrategy


class TestNEWTStrategy(unittest.TestCase):
    """
    Unit tests for the NEWTStrategy class.
    """
    @classmethod
    def setUp(cls):
        """
        Set up the test environment.

        Attributes:
            strategy: The CTAStrategy object for testing.
        """
        cls.strategy = NEWTStrategy(
            name='newt',
            position_size=0.05,
        )
    
    def test_init(self):
        """
        Test the initialisation of the NEWTStrategy object.

        Asserts:
            The object is correctly initialised.
        """
        self.assertEqual(self.strategy.name, 'newt')
        self.assertEqual(self.strategy.position_size, 0.05)

    @patch('core.utils.connect_db')
    def test_get_strategy_returns(self, mock_connect_db):
        """
        Test the computation of strategy returns.

        Args:
            mock_connect_db: The mock object for the connect_db function.

        Asserts:
            The returns are computed correctly.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect_db.return_value = (mock_conn, mock_cursor)
        mock_cursor.description = [
            ('price_at_record',),
            ('price_plus_30min',),
            ('price_plus_1hr',),
            ('price_plus_3hr',),
            ('price_eod',),
            ('record_timestamp',)
        ]
        mock_cursor.fetchall.return_value = [
            (100, 102, 104, 108, 110, '2024-11-15 09:30:00'),
            (200, 202, 206, 210, 215, '2024-11-15 10:30:00')
        ]
        self.strategy.set_data()
        returns = self.strategy.get_strategy_returns()
        expected_returns = pd.Series(
            data=[0.0087],
            index=[pd.to_datetime('2024-11-15').date()],
            name='signal_return'
        )
        expected_returns.index.name = 'date'
        pd.testing.assert_series_equal(returns, expected_returns)

    def test_get_strategy_output(self):
        """
        Test the strategy output generation.

        Asserts:
            The output contains the 'Strategy Levels' key.
            The 'Strategy Levels' key contains the expected DataFrame.
        """
        mock_returns = pd.Series(
            data=[0.05, 0.02],
            index=[pd.Timestamp('2024-11-13'), pd.Timestamp('2024-11-14')]
        )
        self.strategy.get_strategy_returns = MagicMock(return_value=mock_returns)
        output = self.strategy.get_strategy_output()
        expected_output = {
            'Strategy Levels': (1 + mock_returns).cumprod()
        }
        self.assertIsInstance(output, dict)
        for key in expected_output.keys():
            self.assertIn(key, output)
            self.assertIsInstance(output[key], pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
