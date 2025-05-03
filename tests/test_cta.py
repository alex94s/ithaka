# -*- coding: utf-8 -*-
"""
Module Name: test_cta.py

Description:
This module contains the unit tests for the CTA module. The unit tests in this 
module ensure the correctness and reliability of the functions utilised by using
mock objects to simulate database interactions and other dependencies.

Author: elreysausage
Date: 2024-10-29
"""

import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from strategies.cta import CTAStrategy


class TestCTA(unittest.TestCase):
    """
    Unit tests for the CTA module.

    Attributes:
        strategy: The CTAStrategy object for testing.
        sample_returns: Sample return data for testing.
        sample_weights: Sample weights for testing.
    """
    @classmethod
    def setUp(cls):
        """
        Set up the test environment.

        Attributes:
            strategy: The CTAStrategy object for testing.
            sample_returns: Sample return data for testing.
            sample_weights: Sample weights for testing.
        """
        cls.strategy = CTAStrategy(
            name='cta',
            lookback_window=126,
            rebal_freq=126,
            target_vol=0.2
        )
        cls.sample_returns = pd.DataFrame({
            'CSPX': [0.01, 0.02, 0.03, 0.04],
            'MES': [0.05, 0.06, 0.07, 0.08]
        })
        cls.sample_weights = np.array([0.5, 0.5])

    def test_init(self):
        """
        Tests the initialization of the CTAStrategy class.

        Asserts:
            name: The name of the strategy.
            lookback_window: The lookback window of the strategy.
            rebal_freq: The rebalancing frequency of the strategy.
            target_vol: The target volatility of the strategy.
        """
        self.assertEqual(self.strategy.name, 'cta')
        self.assertEqual(self.strategy.lookback_window, 126)
        self.assertEqual(self.strategy.rebal_freq, 126)
        self.assertEqual(self.strategy.target_vol, 0.2)

    @patch('core.utils.get_prices')
    def test_get_autocorrelation_returns(self, mock_get_prices):
        """
        Tests the get_autocorrelation_returns method.

        Parameters:
            mock_get_prices: The mock object for get_prices.

        Asserts:
            strategy_returns: The Series returned by get_autocorrelation_returns.
            signals: The Series returned by get_autocorrelation_returns.
        """
        mock_get_prices.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'CSPX': [396.09, 394.28]
        }).set_index('date')
        strategy_returns, signals = self.strategy.get_autocorrelation_returns(
            tickers=['CSPX'],
            start_date='2023-01-02',
            end_date='2023-01-03'
        )
        self.assertIsInstance(strategy_returns, pd.Series)
        self.assertIsInstance(signals, pd.Series)

    @patch('core.utils.get_prices')
    @patch('core.utils.get_daily_returns')
    def test_get_trend_returns(self, mock_get_daily_returns, mock_get_prices):
        """
        Tests the get_trend_returns method.

        Parameters:
            mock_get_daily_returns: The mock object for get_daily_returns.
            mock_get_prices: The mock object for get_prices.

        Asserts:
            strategy_returns: The Series returned by get_trend_returns.
            signals: The DataFrame returned by get_trend_returns.
        """
        mock_get_prices.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'CSPX': [396.09, 394.28]
        }).set_index('date')
        mock_get_daily_returns.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'CSPX': [0.01, 0.02]
        }).set_index('date')
        strategy_returns, signals = self.strategy.get_trend_returns(
            tickers=['CSPX'],
            start_date='2023-01-02',
            end_date='2023-01-03',
            enable_shorts=True
        )
        self.assertIsInstance(strategy_returns, pd.Series)
        self.assertIsInstance(signals, pd.DataFrame)

    @patch('core.utils.get_prices')
    @patch('core.utils.get_daily_returns')
    def test_get_seasonality_returns(self, mock_get_daily_returns, mock_get_prices):
        """
        Tests the get_seasonality_returns method.

        Parameters:
            mock_get_daily_returns: The mock object for get_daily_returns.
            mock_get_prices: The mock object for get_prices.

        Asserts:
            strategy_returns: The Series returned by get_seasonality_returns.
            signals: The DataFrame returned by get_seasonality_returns.
        """
        mock_get_prices.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'CSPX': [396.09, 394.28]
        }).set_index('date')
        mock_get_daily_returns.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'CSPX': [0.01, 0.02]
        }).set_index('date')
        strategy_returns, signals = self.strategy.get_seasonality_returns(
            tickers=['CSPX'],
            buy_months=[1, 2, 3],
            start_date='2023-01-02',
            end_date='2023-01-03'
        )
        self.assertIsInstance(strategy_returns, pd.Series)
        self.assertIsInstance(signals, pd.DataFrame)

    @patch('core.utils.get_prices')
    @patch('core.utils.get_daily_returns')
    def test_get_commodity_seasonality_returns(self, mock_get_daily_returns, mock_get_prices):
        """
        Tests the get_commodity_seasonality_returns method.

        Parameters:
            mock_get_daily_returns: The mock object for get_daily_returns.
            mock_get_prices: The mock object for get_prices.

        Asserts:
            strategy_returns: The Series returned by get_commodity_seasonality_returns.
            signals: The DataFrame returned by get_commodity_seasonality_returns.
        """
        mock_get_prices.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'CSPX': [396.09, 394.28]
        }).set_index('date')
        mock_get_daily_returns.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'CSPX': [0.01, 0.02]
        }).set_index('date')
        strategy_returns, signals = self.strategy.get_commodity_seasonality_returns(
            ags_params=[['CSPX'], [1, 2, 3], '2023-01-02', '2023-01-03'],
            energy_params=[['CSPX'], [4, 5, 6], '2023-01-02', '2023-01-03']
        )
        self.assertIsInstance(strategy_returns, pd.Series)
        self.assertIsInstance(signals, pd.DataFrame)

    @patch('core.utils.get_prices')
    @patch('core.utils.get_daily_returns')
    def test_get_insurance_returns(self, mock_get_daily_returns, mock_get_prices):
        """
        Tests the get_insurance_returns method.

        Parameters:
            mock_get_daily_returns: The mock object for get_daily_returns.
            mock_get_prices: The mock object for get_prices.

        Asserts:
            strategy_returns: The Series returned by get_insurance_returns.
            signals: The DataFrame returned by get_insurance_returns.
        """
        mock_get_prices.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'VIX': [20, 21]
        }).set_index('date')

        mock_get_daily_returns.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'VIXM': [0.01, 0.02]
        }).set_index('date')
        
        strategy_returns, signals = self.strategy.get_insurance_returns(
            instrument_ticker=['VIXM'],
            vix_ticker=['VIX'],
            start_date='2023-01-02',
            end_date='2023-01-03'
        )
        self.assertIsInstance(strategy_returns, pd.Series)
        self.assertIsInstance(signals, pd.DataFrame)

    def test_get_leverage_factor(self):
        """
        Tests the get_leverage_factor method.

        Asserts:
            leverage_factor: The Series returned by get_leverage_factor.
        """
        leverage_factor = self.strategy.get_leverage_factor(
            self.sample_returns['CSPX'],
            self.strategy.rebal_freq,
            'expanding'
        )
        self.assertIsInstance(leverage_factor, pd.Series)

    def test_get_strategy_returns(self):
        """
        Tests the get_strategy_returns method.

        Asserts:
            strategy_returns: The Series returned by get_strategy_returns.
        """
        self.strategy.sub_strategy_returns = self.sample_returns
        self.strategy.sub_strategy_weights = pd.DataFrame({
            'CSPX': [0.5, 0.5, 0.5, 0.5],
            'MES': [0.5, 0.5, 0.5, 0.5]
        })
        strategy_returns = self.strategy.get_strategy_returns()
        self.assertIsInstance(strategy_returns, pd.Series)

    def test_get_strategy_output(self):
        """
        Tests the get_strategy_output method.

        Asserts:
            output: The dictionary returned by get_strategy_output
        """
        self.strategy.sub_strategy_returns = self.sample_returns
        self.strategy.sub_strategy_weights = pd.DataFrame({
            'CSPX': [0.5, 0.5, 0.5, 0.5],
            'MES': [0.5, 0.5, 0.5, 0.5]
        })
        output = self.strategy.get_strategy_output()
        self.assertIsInstance(output, dict)
        self.assertIn('Strategy Levels', output)
        self.assertIn('Sub-strategy Levels', output)
        self.assertIn('Target Weights', output)
        self.assertIn('Effective Weights', output)


if __name__ == '__main__':
    unittest.main()