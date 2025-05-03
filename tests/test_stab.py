# -*- coding: utf-8 -*-
"""
Module Name: test_stab.py

Description:
This module contains the unit tests for the STAB module. The unit tests in this 
module ensure the correctness and reliability of the functions utilised by using
mock objects to simulate database interactions and other dependencies.

Author: elreysausage
Date: 2024-11-19
"""

from datetime import date
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from strategies.stab import STABStrategy


class TestSTABStrategy(unittest.TestCase):
    """
    Unit tests for the NEWTStrategy class.
    """
    @patch('core.utils.get_prices')
    def setUp(self, mock_get_prices):
        """
        Set up the test environment.

        Attributes:
            strategy: The STABStrategy object for testing.
            tickers: The list of tickers for testing.
            start_date: The start date for testing.
            end_date: The end date for testing.
            n_clusters: The number of clusters for testing.
            n_sub_strategies: The number of sub-strategies for testing.
        """
        self.tickers = ['AAP', 'AAT', 'ABCB']
        self.start_date = '2015-01-01'
        self.end_date = date.today()
        self.n_clusters = 2
        self.n_sub_strategies = 1

        dates = pd.date_range(start=self.start_date, end=self.end_date)
        mock_get_prices.return_value = (
                pd.DataFrame(
                {
                    'AAP': np.random.randint(100, 200, size=len(dates)),
                    'AAT': np.random.randint(50, 150, size=len(dates)),
                    'ABCB': np.random.randint(200, 300, size=len(dates))
                },
                index=dates)
        )
        self.strategy = STABStrategy(
            name='stab',
            n_clusters=self.n_clusters,
            n_sub_strategies=self.n_sub_strategies
        )

    def test_set_data(self):
        """
        Test the set_data method.

        Asserts:
            price_data: The price data DataFrame.
            returns_data: The returns data DataFrame.
        """
        self.strategy.set_data()
        self.assertIsNotNone(self.strategy.price_data)
        self.assertIsNotNone(self.strategy.returns_data)

    def test_get_reversion_signals(self):
        """
        Test the get_reversion_signals method.

        Asserts:
            signals: The signals DataFrame.
        """
        signals = self.strategy.get_reversion_signals(
            self.tickers, 
            start_date=pd.Timestamp('2023-01-01'), 
            end_date=pd.Timestamp('2023-01-02')
        )
        self.assertIsNotNone(signals)
        self.assertIsInstance(signals, pd.DataFrame)
        self.assertEqual(signals.shape[1], len(self.tickers))

    @patch('strategies.stab.STABStrategy.get_reversion_signals')
    def test_get_sub_strategy_returns(self, mock_get_reversion_signals):
        """
        Test the get_sub_strategy_returns method.

        Parameters:
            mock_get_reversion_signals: The mock object for get_reversion_signals.

        Asserts:
            returns: DataFrame returned by get_sub_strategy_returns.
        """
        mock_get_reversion_signals.return_value = pd.DataFrame(
            {
                'AAP': [0.1, -0.1],
                'AAT': [0.2, -0.2],
                'ABCB': [0.3, -0.3]
            },
            index=pd.date_range(start='2023-01-01', periods=2)
        )
        returns = self.strategy.get_sub_strategy_returns(
            self.tickers, 
            start_date=pd.Timestamp('2023-01-01'), 
            end_date=pd.Timestamp('2023-01-02')
        )
        self.assertIsNotNone(returns)

    @patch('sklearn.cluster.KMeans.fit_predict')
    def test_get_ticker_clusters(self, mock_fit_predict):
        """
        Test the get_ticker_clusters method.

        Parameters:
            mock_fit_predict: The mock object for KMeans.fit_predict.

        Asserts:
            clusters: The clusters dictionary returned by get_ticker_clusters.
        """
        mock_fit_predict.return_value = [0, 1, 0]
        start_date = pd.Timestamp('2023-01-01')
        end_date = pd.Timestamp('2023-01-02')
        clusters = self.strategy.get_ticker_clusters(start_date, end_date)
        self.assertEqual(len(clusters), self.strategy.n_clusters)

    @patch('strategies.stab.STABStrategy.get_sub_strategy_returns')
    def test_merge_sub_strategy_returns(self, mock_get_sub_strategy_returns):
        """
        Test the merge_sub_strategy_returns method.

        Parameters:
            mock_get_sub_strategy_returns: Mock object for get_sub_strategy_returns.

        Asserts:
            merged_returns: The merged returns DataFrame.
        """
        mock_get_sub_strategy_returns.return_value = pd.Series(
            [0.1, 0.2],
            index=pd.date_range(start='2023-01-01', periods=2)
        )
        merged_returns = self.strategy.merge_sub_strategy_returns(
            clusters= {0: ['AAP', 'AAT'], 1: ['ABCB']}, 
            start_date=pd.Timestamp('2023-01-01'), 
            end_date=pd.Timestamp('2023-01-02')
        )
        self.assertIsNotNone(merged_returns)

    @patch('strategies.stab.STABStrategy.merge_sub_strategy_returns')
    def test_identify_top_clusters(self, mock_merge_sub_strategy_returns):
        """
        Test the identify_top_clusters method.

        Parameters:
            mock_merge_sub_strategy_returns: Mock object for merge_sub_strategy_returns.
        
        Asserts:
            top_clusters: The top clusters DataFrame.
        """
        mock_clusters = pd.DataFrame({
            'Cluster': [0, 1],
            'Sharpe Ratio': [1.5, 2.0],
            'Tickers': [['AAP', 'AAT'], ['ABCB']]
        })
        mock_merge_sub_strategy_returns.return_value = mock_clusters
        top_clusters = self.strategy.identify_top_clusters(
            clusters={0: ['AAP', 'AAT'], 1: ['ABCB']}, 
            start_date=pd.Timestamp('2023-01-01'), 
            end_date=pd.Timestamp('2023-01-02')
        )
        expected_top_clusters = mock_clusters.sort_values(
            by="Sharpe Ratio", ascending=False).head(self.strategy.n_sub_strategies)
        pd.testing.assert_frame_equal(top_clusters, expected_top_clusters)

    @patch('strategies.stab.STABStrategy.identify_top_clusters')
    def test_get_strategy_returns(self, mock_identify_top_clusters):
        """
        Test the get_strategy_returns method.
        
        Parameters:
            mock_identify_top_clusters: Mock object for identify_top_clusters.

        Asserts:
            strategy_returns: The strategy returns Series.
        """
        mock_identify_top_clusters.return_value = pd.DataFrame(
            {
                'Cluster': [0],
                'Sub-strategy Returns': [pd.Series([0.1, 0.2])]
            }
        )
        strategy_returns = self.strategy.get_strategy_returns(
            mock_identify_top_clusters()
        )
        self.assertIsNotNone(strategy_returns)

    @patch('strategies.stab.STABStrategy.identify_top_clusters')
    def test_get_strategy_weights(self, mock_identify_top_clusters):
        """
        Test the get_strategy_weights method.

        Parameters:
            mock_identify_top_clusters: Mock object for identify_top_clusters.
        
        Asserts:
            strategy_weights: The strategy weights DataFrame.
        """
        mock_identify_top_clusters.return_value = pd.DataFrame(
            {
                'Cluster': [0],
                'Instrument Weights': [pd.DataFrame(
                    {
                        'AAP': [0.5, 0.5],
                        'AAT': [0.5, 0.5]
                    },
                    index=pd.date_range(start='2023-01-01', periods=2)
                )]
            }
        )
        strategy_weights = self.strategy.get_strategy_weights(
            mock_identify_top_clusters()
        )
        self.assertIsNotNone(strategy_weights)

    @patch('strategies.stab.STABStrategy.get_strategy_returns')
    @patch('strategies.stab.STABStrategy.get_strategy_weights')
    def test_get_strategy_output(
        self, 
        mock_get_strategy_weights, 
        mock_get_strategy_returns
    ):
        """
        Test the get_strategy_output method.

        Parameters:
            mock_get_strategy_weights: Mock object for get_strategy_weights.
            mock_get_strategy_returns: Mock object for get_strategy_returns.
        
        Asserts:
            output: The strategy output dictionary.
        """
        mock_get_strategy_returns.return_value = pd.Series(
            [0.1, 0.2],
            index=pd.date_range(start='2023-01-01', periods=2)
        )
        mock_get_strategy_weights.return_value = pd.DataFrame(
            {
                'AAP': [0.5, 0.5],
                'AAT': [0.5, 0.5]
            },
            index=pd.date_range(start='2023-01-01', periods=2)
        )
        output = self.strategy.get_strategy_output()
        expected_keys = ['Strategy Levels', 'Target Weights', 'Effective Weights']
        self.assertIsInstance(output, dict)
        for key in expected_keys:
            self.assertIn(key, output)
            self.assertIsInstance(output[key], (pd.Series, pd.DataFrame))


if __name__ == '__main__':
    unittest.main()