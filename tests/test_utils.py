# -*- coding: utf-8 -*-
"""
Module Name: test_utils.py

Description:
This module contains the unit tests for the Utils module. The unit tests in this 
module ensure the correctness and reliability of the functions utilised by using
mock objects to simulate database interactions and other dependencies.

Author: elreysausage
Date: 2024-09-30
"""

import unittest
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd

import core.utils as ut


class TestUtils(unittest.TestCase):
    """
    Unit tests for the Utils module.

    Attributes:
        sample_prices: Sample price data for testing.
        sample_returns: Sample return data for testing.
        sample_weights: Sample weights for testing.
        rebal_freq: Rebalancing frequency for testing.
    """
    @classmethod
    def setUp(cls):
        """
        Set up the test environment.

        Attributes:
            sample_prices: Sample price data for testing.
            sample_returns: Sample return data for testing.
            sample_weights: Sample weights for testing.
            rebal_freq: Rebalancing frequency for testing.
        """
        cls.sample_prices = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'CSPX': [396.09, 394.28],
            'MES': [4122.05, 4106.04]
        }).set_index('date')
        cls.sample_prices.columns.name = 'symbol'
        cls.sample_returns = pd.DataFrame({
            'CSPX': [0.01, 0.02, 0.03, 0.04],
            'MES': [0.05, 0.06, 0.07, 0.08]
        })
        cls.sample_weights = np.array([0.5, 0.5])
        cls.rebal_freq = 2

    @patch('mysql.connector.connect')
    def test_get_prices_success(self, mock_connect):
        """
        Tests that get_prices returns the correct DataFrame structure.

        Parameters:
            mock_cursor (MagicMock): The mock cursor object.

        Asserts:
            price_data: The DataFrame returned by get_prices.
            self.sample_prices: The expected DataFrame.
        """
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_connect.return_value = mock_conn
        mock_cursor.fetchall.return_value = [
            ('CSPX', '2023-01-02', 396.09, 'YHOO'),
            ('CSPX', '2023-01-03', 394.28, 'YHOO'),
            ('MES', '2023-01-02', 4122.05, 'IBKR'),
            ('MES', '2023-01-03', 4106.04, 'IBKR')
        ]
        price_data = ut.get_prices(
            tickers=['CSPX', 'MES'],
            start_date='2023-01-02',
            end_date='2023-01-03'
        )
        mock_cursor.fetchall.return_value = [
            ('CSPX', '2023-01-02', 396.09, 'YHOO'),
            ('CSPX', '2023-01-03', 394.28, 'YHOO'),
            ('MES', '2023-01-02', 4122.05, 'IBKR'),
            ('MES', '2023-01-03', 4106.04, 'IBKR')
        ]
        pd.testing.assert_frame_equal(price_data, self.sample_prices)

    @patch('mysql.connector.connect')
    def test_get_prices_failure(self, mock_connect):
        """
        Tests that get_prices raises an error when the database query fails.

        Parameters:
            mock_cursor (MagicMock): The mock cursor object.

        Asserts:
            error: The exception raised by get_prices.
        """
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_connect.return_value = mock_conn
        mock_cursor.fetchall.side_effect = Exception('No records found.')
        with self.assertRaises(Exception) as error:
            ut.get_prices(
                tickers=['FALSE_TICKER'],
                start_date='2023-01-02',
                end_date='2023-01-03'
            )
        self.assertEqual(str(error.exception), 'Price data unavailable...')

    def test_set_rebal_dates(self):
        """
        Tests that set_rebal_dates returns the correct Series structure.

        Asserts:
            rebal_dates (Series): The Series returned by set_rebal_dates.
            expected_rebal_dates: The expected Series.
        """
        rebal_dates = ut.set_rebal_dates(self.sample_returns, self.rebal_freq)
        expected_rebal_dates = pd.Series(
            [1, 0, 1, 0], index=self.sample_returns.index)
        pd.testing.assert_series_equal(rebal_dates, expected_rebal_dates)

    def test_get_portfolio_weights(self):
        """
        Tests that get_portfolio_weights returns the correct DataFrame structure.

        Asserts:
            portfolio_weights: The DataFrame returned by get_portfolio_weights.
        """
        portfolio_weights = ut.get_portfolio_weights(
            strategy_returns=self.sample_returns,
            rebal_freq=self.rebal_freq,
            weighting_scheme='equal',
            training_method='expanding',
            instrument_returns=self.sample_returns
        )
        self.assertIsInstance(portfolio_weights, pd.DataFrame)

    def test_get_portfolio_variance(self):
        """
        Tests that get_portfolio_variance returns the correct variance.

        Asserts:
            variance: The variance returned by get_portfolio_variance.
        """
        variance = ut.get_portfolio_variance(
            self.sample_weights, self.sample_returns)
        self.assertIsInstance(variance, float)

    def test_get_portfolio_sharpe(self):
        """
        Tests that get_portfolio_sharpe returns the correct Sharpe ratio.

        Asserts:
            sharpe_ratio: The Sharpe ratio returned by get_portfolio_sharpe.
        """
        sharpe_ratio = ut.get_portfolio_sharpe(
            self.sample_weights, self.sample_returns)
        self.assertIsInstance(sharpe_ratio, float)

    def test_get_excess_risk_contributions(self):
        """
        Tests that get_excess_risk_contributions returns the correct value.

        Asserts:
            excess_risk_contributions (float): The excess risk contributions 
                returned by get_excess_risk_contributions.
        """
        excess_risk_contributions = ut.get_excess_risk_contributions(
            self.sample_weights, self.sample_returns
        )
        self.assertIsInstance(excess_risk_contributions, float)

    def test_get_weight_constraint(self):
        """
        Tests that get_weight_constraint returns the correct value.

        Asserts:
            constraint_value: Weight constraint returned by get_weight_constraint.
        """
        constraint_value = ut.get_weight_constraint(self.sample_weights)
        self.assertEqual(constraint_value, 0)

    def test_get_rebal_weights_equal(self):
        """
        Tests that get_rebal_weights returns the correct weights
        for the equal weighting scheme.

        Asserts:
            rebal_weights: Rebalancing weights returned by get_rebal_weights.
        """
        rebal_weights = ut.get_rebal_weights(
            self.sample_returns,
            weighting_scheme='equal'
        )
        self.assertIsInstance(rebal_weights, list)
        self.assertEqual(len(rebal_weights), len(self.sample_returns.columns))
        self.assertEqual(sum(rebal_weights), 1)

    def test_get_rebal_weights_min_variance(self):
        """
        Tests that get_rebal_weights returns the correct weights
        for the min_variance weighting scheme.

        Asserts:
            rebal_weights: Rebalancing weights returned by get_rebal_weights.
        """
        rebal_weights = ut.get_rebal_weights(
            self.sample_returns,
            weighting_scheme='min_variance'
        )
        self.assertIsInstance(rebal_weights, list)
        self.assertEqual(len(rebal_weights), len(self.sample_returns.columns))
        self.assertEqual(sum(rebal_weights), 1)

    def test_get_rebal_weights_max_sharpe(self):
        """
        Tests that get_rebal_weights returns the correct weights
        for the max_sharpe weighting scheme.

        Asserts:
            rebal_weights: Rebalancing weights returned by get_rebal_weights.
        """
        rebal_weights = ut.get_rebal_weights(
            self.sample_returns,
            weighting_scheme='max_sharpe'
        )
        self.assertIsInstance(rebal_weights, list)
        self.assertEqual(len(rebal_weights), len(self.sample_returns.columns))
        self.assertEqual(sum(rebal_weights), 1)

    def test_get_rebal_weights_risk_parity(self):
        """
        Tests that get_rebal_weights returns the correct weights
        for the risk_parity weighting scheme.

        Asserts:
            rebal_weights: Rebalancing weights returned by get_rebal_weights.
        """
        rebal_weights = ut.get_rebal_weights(
            self.sample_returns,
            weighting_scheme='risk_parity'
        )
        self.assertIsInstance(rebal_weights, list)
        self.assertEqual(len(rebal_weights), len(self.sample_returns.columns))
        self.assertEqual(sum(rebal_weights), 1)

    def test_get_rebal_weights_failure(self):
        """
        Tests that get_rebal_weights raises a ValueError when 
        an invalid weighting scheme is passed.

        Asserts:
            Exception message is raised when an invalid scheme is provided.
        """
        with self.assertRaises(ValueError) as error:
            ut.get_rebal_weights(
                self.sample_returns,
                weighting_scheme='FALSE_SCHEME'
            )
        self.assertEqual(
            str(error.exception), 
                "Invalid weighting scheme - choose from 'equal', "
                "'min_variance', 'max_sharpe', or 'risk_parity'..."
        )


if __name__ == '__main__':
    unittest.main()
