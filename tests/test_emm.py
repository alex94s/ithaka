# -*- coding: utf-8 -*-
"""
Module Name: test_emm.py

Description:
This module contains the unit tests for the EMM module. The unit tests in this 
module ensure the correctness and reliability of the functions utilised by using
mock objects to simulate database interactions and other dependencies.

Author: elreysausage
Date: 2024-10-29
"""

import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from strategies.emm import EMMStrategy


class TestEMM(unittest.TestCase):
    """
    Unit tests for the EMM module.

    Attributes:
        strategy: The EMMStrategy object for testing.
        sample_returns: Sample return data for testing.
        sample_weights: Sample weights for testing.
    """
    @classmethod
    def setUp(cls):
        """
        Set up the test environment.

        Attributes:
            strategy: The EMMStrategy object for testing.
            sample_returns: Sample return data for testing.
            sample_weights: Sample weights for testing.
        """
        cls.strategy = EMMStrategy(
            name='emm',
            lookback_window=126,
            n_stocks=25,
            rebal_freq=126
        )
        cls.sample_returns = pd.DataFrame({
            '360ONE.NS': [0.01, 0.02, 0.03, 0.04],
            '3MINDIA.NS': [0.05, 0.06, 0.07, 0.08]
        })
        cls.sample_weights = np.array([0.5, 0.5])
        cls.strategy.effective_weights = pd.DataFrame({
            'equity': [0.2, 0.3],
            'fx': [0.1, 0.2]
        }, index=pd.to_datetime(['2023-01-02', '2023-01-03']))
        
        cls.strategy.drifted_equity_hedge_ratio = (
            pd.Series([-0.05, -0.05], index=cls.strategy.effective_weights.index)
        )
        cls.strategy.drifted_fx_hedge_ratio = (
            pd.Series([-0.1, -0.1], index=cls.strategy.effective_weights.index)
        )
        cls.strategy.target_weights = pd.DataFrame({
            'equity': [0.25, 0.35],
            'fx': [0.15, 0.25]
        }, index=pd.to_datetime(['2023-01-02', '2023-01-03']))

        cls.strategy.equity_hedge_ratio = pd.Series(
            [0.05, 0.05], index=cls.strategy.target_weights.index)
        cls.strategy.fx_hedge_ratio = pd.Series(
            [0.1, 0.1], index=cls.strategy.target_weights.index)

    def test_init(self):
        """
        Tests the initialization of the EMMStrategy class.

        Asserts:
            name: The name of the strategy.
            lookback_window: The lookback window of the strategy.
            n_stocks: The number of stocks in the strategy.
            rebal_freq: The rebalancing frequency of the strategy.
        """
        self.assertEqual(self.strategy.name, 'emm')
        self.assertEqual(self.strategy.lookback_window, 126)
        self.assertEqual(self.strategy.n_stocks, 25)
        self.assertEqual(self.strategy.rebal_freq, 126)

    @patch('core.utils.get_prices')
    def test_get_equity_returns(self, mock_get_prices):
        """
        Tests the get_equity_returns method.

        Parameters:
            mock_get_prices: The mock object for get_prices.

        Asserts:
            equity_returns: The Series returned by get_equity_returns.
        """
        mock_get_prices.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            '360ONE.NS': [100, 102],
            '3MINDIA.NS': [200, 204]
        }).set_index('date')
        self.strategy.get_equity_returns()
        self.assertIsInstance(self.strategy.equity_returns, pd.Series)

    def test_get_momentum_score(self):
        """
        Tests the get_momentum_score method.

        Asserts:
            momentum_score: The DataFrame returned by get_momentum_score.
        """
        price_data = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            '360ONE.NS': [100, 102],
            '3MINDIA.NS': [200, 204]
        }).set_index('date')
        momentum_score = self.strategy.get_momentum_score(price_data)
        self.assertIsInstance(momentum_score, pd.DataFrame)

    def test_get_strategy_weights(self):
        """
        Tests the get_strategy_weights method.

        Asserts:
            effective_weights: The DataFrame returned by get_strategy_weights.
        """
        self.strategy.target_weights = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            '360ONE.NS': [0.5, 0.5],
            '3MINDIA.NS': [0.5, 0.5]
        }).set_index('date')
        self.strategy.daily_returns = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            '360ONE.NS': [0.01, 0.02],
            '3MINDIA.NS': [0.03, 0.04]
        }).set_index('date')
        self.strategy.rebal_dates = pd.Series(
            [1, 0], index=pd.to_datetime(['2023-01-02', '2023-01-03']))
        effective_weights = self.strategy.get_strategy_weights()
        self.assertIsInstance(effective_weights, pd.DataFrame)

    @patch('core.utils.get_prices')
    def test_get_hedge_returns(self, mock_get_prices):
        """
        Tests the get_hedge_returns method.

        Parameters:
            mock_get_prices: The mock object for get_prices.

        Asserts:
            hedge_returns: The Series returned by get_hedge_returns.
        """
        mock_get_prices.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'ES=F': [396.09, 394.28]
        }).set_index('date')
        self.strategy.equity_returns = pd.Series(
            [0.01, 0.02], index=pd.to_datetime(['2023-01-02', '2023-01-03']))
        hedge_returns = self.strategy.get_hedge_returns()
        self.assertIsInstance(hedge_returns, pd.Series)

    def test_get_strategy_output(self):
        """
        Tests the get_strategy_output method of the EMMStrategy.

        Asserts:
            output: The dictionary returned by get_strategy_output.
        """
        with patch(
            'strategies.emm.EMMStrategy.get_strategy_returns', 
            return_value=pd.Series(
                [0.01, 0.02], index=pd.to_datetime(['2023-01-02', '2023-01-03']))
        ) as mock_get_returns, patch(
            'strategies.emm.EMMStrategy.get_strategy_levels', 
            return_value=pd.Series(
                [100, 102], index=pd.to_datetime(['2023-01-02', '2023-01-03']))
        ) as mock_get_levels:
            output = self.strategy.get_strategy_output()
            self.assertIsInstance(output, dict)
            self.assertIn('Strategy Levels', output)
            self.assertIn('Target Weights', output)
            self.assertIn('Effective Weights', output)


if __name__ == '__main__':
    unittest.main()