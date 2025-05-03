# -*- coding: utf-8 -*-
"""
Module Name: test_far.py

Description:
This module contains the unit tests for the FAR module. The unit tests in this 
module ensure the correctness and reliability of the functions utilised by using
mock objects to simulate database interactions and other dependencies.

Author: elreysausage
Date: 2025-02-23
"""

from datetime import date
import unittest
from unittest.mock import patch, MagicMock

import pandas as pd

from strategies.far import FARStrategy


class TestFARStrategy(unittest.TestCase):
    """
    Unit tests for the FARStrategy class.
    """
    def setUp(self) -> None:
        """
        Set up the test environment.

        Attributes:
            start_date: The start date for data retrieval.
            end_date: The end date for data retrieval.
            tickers: The tickers for the strategy.
            price_data: The price data for the strategy.
            strategy: The FAR strategy object to test.
        """
        self.start_date = '2000-01-01'
        self.end_date = date.today()
        self.tickers = ['JNK', 'FALN']
        dates = pd.date_range(
            start=self.start_date, 
            end=self.end_date
        )
        self.price_data = pd.DataFrame(
            {
                'JNK': range(len(dates)),
                'FALN': range(len(dates))
            },
            index=dates
        )
        self.strategy = FARStrategy(name='far')

    @patch('core.utils.get_prices')
    def test_get_strategy_weights(self, mock_get_prices: MagicMock) -> None:
        """
        Test the get_strategy_weights method.
        """
        mock_get_prices.return_value = self.price_data
        self.strategy.set_data()
        weights = self.strategy.get_strategy_weights()
        self.assertIsNotNone(weights)
        self.assertIsInstance(weights, pd.DataFrame)
        self.assertEqual(weights.shape[1], len(self.tickers))

    @patch('core.utils.get_prices')
    def test_get_strategy_returns(self, mock_get_prices: MagicMock) -> None:
        """
        Test the get_strategy_returns method.
        """
        mock_get_prices.return_value = self.price_data
        self.strategy.set_data()
        returns = self.strategy.get_strategy_returns()
        self.assertIsNotNone(returns)
        self.assertIsInstance(returns, pd.Series)

    @patch('core.utils.get_prices')
    def test_get_strategy_output(self, mock_get_prices: MagicMock) -> None:
        """
        Test the get_strategy_output method.
        """
        mock_get_prices.return_value = self.price_data
        self.strategy.set_data()
        output = self.strategy.get_strategy_output()
        expected_keys = ['Strategy Levels', 'Target Weights', 'Effective Weights']
        self.assertIsInstance(output, dict)
        for key in expected_keys:
            self.assertIn(key, output)
            self.assertIsInstance(output[key], (pd.Series, pd.DataFrame))


if __name__ == '__main__':
    unittest.main()