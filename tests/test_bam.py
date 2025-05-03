# -*- coding: utf-8 -*-
"""
Module Name: test_bam.py

Description:
This module contains the unit tests for the BAM module. The unit tests in this 
module ensure the correctness and reliability of the functions utilised by using
mock objects to simulate database interactions and other dependencies.

Author: elreysausage
Date: 2024-09-30
"""

import unittest
from unittest.mock import patch

import pandas as pd

from strategies.bam import BAMStrategy


class TestBAM(unittest.TestCase):
    """
    Unit tests for the BAM module.

    Attributes:
        strategy: The BAMStrategy object for testing.
    """
    @classmethod
    def setUp(cls):
        """
        Set up the test environment.

        Attributes:
            strategy: The BAMStrategy object for testing.
        """
        cls.strategy = BAMStrategy(
            name='bam',
            lookback_window=1,
            rebal_freq=126,
            signal_update_freq=22
        )

    def test_init(self):
        """
        Tests the initialization of the BAMStrategy class.

        Asserts:
            name: The name of the strategy.
            lookback_window: The lookback window of the strategy.
            rebal_freq: The rebalancing frequency of the strategy.
            signal_update_freq: The signal update frequency of the strategy.
        """
        self.assertEqual(self.strategy.name, 'bam')
        self.assertEqual(self.strategy.lookback_window, 1)
        self.assertEqual(self.strategy.rebal_freq, 126)
        self.assertEqual(self.strategy.signal_update_freq, 22)

    @patch('core.utils.get_prices')
    @patch('core.utils.set_rebal_dates')
    def test_get_signals(self, mock_set_rebal_dates, mock_get_prices):
        """
        Tests that get_signals returns the correct DataFrame structure.

        Parameters:
            mock_get_prices: The mock object for get_prices.
            mock_set_rebal_dates: The mock object for set_rebal_dates.

        Asserts:
            trend_signals: The DataFrame returned by get_signals.
            expected_signals: The expected DataFrame returned by get_signals.
        """
        mock_get_prices.return_value = pd.DataFrame({
            'CSPX': [100.02, 102.33, 104.24, 106.05],
            'MES': [201.02, 198.54, 202.33, 204.54]
        }, index=pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']))

        mock_set_rebal_dates.return_value = pd.Series(
            [0, 1, 0, 1],
            index=pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'])
        )
        trend_signals = self.strategy.get_signals(
            tickers=['CSPX', 'MES'],
            start_date='2023-01-01',
            end_date='2023-01-04'
        ).astype(int)
        
        expected_signals = pd.DataFrame({
            'CSPX': [1, 1, 1],
            'MES': [0, 0, 0]
        }, index=pd.to_datetime(['2023-01-02', '2023-01-03', '2023-01-04']))

        pd.testing.assert_frame_equal(trend_signals, expected_signals)

    def test_get_portfolio_output(self):
        """
        Tests that get_portfolio_output returns the correct dictionary structure.

        Asserts:
            output: The dictionary returned by get_portfolio_output.
        """
        output = self.strategy.get_strategy_output()
        expected_keys = ['Strategy Levels',
                         'Sub-strategy Levels',
                         'Target Weights',
                         'Effective Weights']
        self.assertIsInstance(output, dict)
        for key in expected_keys:
            self.assertIn(key, output)
            self.assertIsInstance(output[key], (pd.Series, pd.DataFrame))


if __name__ == '__main__':
    unittest.main()
