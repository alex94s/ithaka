# -*- coding: utf-8 -*-
"""
Module Name: test_tracker.py

Description:
This module contains the unit tests for the tracker module. The unit tests in this 
module ensure the correctness and reliability of the functions utilized by using
mock objects to simulate database interactions and other dependencies.

Author: elreysausage
Date: 2024-10-30
"""

import unittest
from unittest.mock import patch, MagicMock

import pandas as pd

from core.tracker import (
    get_nav, 
    get_last_prices, 
    get_positions, 
    get_required_trades, 
    set_contract_multipliers
)


class TestTracker(unittest.TestCase):
    """
    Unit tests for the tracker module.
    """
    @patch('core.utils.mysql.connector.cursor.MySQLCursor')
    def test_get_nav(self, mock_cursor):
        """
        Tests the get_nav function.

        Args:
            mock_cursor: The mock cursor object.

        Asserts:
            The net asset value is equal to 100.0.
        """
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = [(100.0)]
        nav = get_nav(mock_cursor)
        self.assertEqual(nav, 100.0)

    @patch('core.utils.mysql.connector.cursor.MySQLCursor')
    def test_get_last_prices(self, mock_cursor):
        """
        Tests the get_last_prices function.

        Args:
            mock_cursor: The mock cursor object.

        Asserts:
            The last prices DataFrame is equal to the expected DataFrame.
        """
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = [('AAPL', 150.0), ('GOOGL', 2800.0)]
        last_prices = get_last_prices(mock_cursor)
        expected_df = pd.DataFrame(
            {'symbol': ['AAPL', 'GOOGL'], 'adj_close': [150.0, 2800.0]})
        pd.testing.assert_frame_equal(last_prices, expected_df)

    @patch('core.utils.mysql.connector.cursor.MySQLCursor')
    @patch('core.tracker.get_nav', return_value=1000.0)
    def test_get_positions(self, mock_get_nav, mock_cursor):
        """
        Tests the get_positions function.

        Args:
            mock_get_nav: The mock get_nav function.
            mock_cursor: The mock cursor object.
        
        Asserts:
            The positions DataFrame is equal to the expected DataFrame.
        """
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = [('AAPL', 0.1), ('GOOGL', 0.2)]
        mock_cursor.description = [('symbol',), ('portfolio_weight',)]
        positions = get_positions(mock_cursor, 'portfolio')
        expected_df = pd.DataFrame(
            {'symbol': ['AAPL', 'GOOGL'], 
             'position': [0.1, 0.2]}).set_index('symbol')
        expected_df *= mock_get_nav.return_value
        pd.testing.assert_frame_equal(positions, expected_df)

    @patch('core.utils.mysql.connector.cursor.MySQLCursor')
    @patch('core.utils.connect_db')
    @patch('core.factory.DataManager')
    @patch('core.tracker.get_nav', return_value=1000.0)
    @patch('core.tracker.get_last_prices')
    @patch('core.tracker.set_contract_multipliers')
    def test_get_required_trades(
        self, 
        mock_set_contract_multipliers, 
        mock_get_last_prices, 
        mock_get_nav, 
        mock_DataManager, 
        mock_connect_db, 
        mock_cursor
    ):
        """
        Tests the get_required_trades function.

        Args:
            mock_set_contract_multipliers: The mock set_contract_multipliers function.
            mock_get_last_prices: The mock get_last_prices function.
            mock_get_nav: The mock get_nav function.
            mock_DataManager: The mock DataManager class.
            mock_connect_db: The mock connect_db function.
            mock_cursor: The mock cursor object.
        
        Asserts:
            The required trades DataFrame is equal to the expected DataFrame.
        """
        mock_connect_db.return_value = (MagicMock(), mock_cursor)
        mock_DataManager.return_value.run_updates.return_value = None
        mock_get_last_prices.return_value = pd.DataFrame(
            {'symbol': ['AAPL', 'GOOGL'], 'adj_close': [150.0, 2800.0]})
        mock_set_contract_multipliers.return_value = pd.DataFrame(
            {'symbol': ['AAPL', 'GOOGL'], 'multiplier': [1, 1]})
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.side_effect = [
            [('AAPL', 0.1), ('GOOGL', 0.2)],  # current_positions
            [('AAPL', 0.15), ('GOOGL', 0.25)]  # target_positions
        ]
        mock_cursor.description = [('symbol',), ('portfolio_weight',)]
        required_trades = get_required_trades(0.01, 'test')
        expected_df = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL'],
            'notional': [50.0, 50.0],
            'adj_close': [150.0, 2800.0],
            'multiplier': [1, 1],
            'action': ['BUY', 'BUY'],
            'quantity': [50.0 / 150.0, 50.0 / 2800.0]
        })
        pd.testing.assert_frame_equal(required_trades, expected_df)

    def test_set_contract_multipliers(self):
        """
        Tests the set_contract_multipliers function.

        Asserts:
            The contract multipliers DataFrame is equal to the expected DataFrame.
        """
        contract_multipliers = set_contract_multipliers()
        expected_df = pd.DataFrame({
            'symbol': ['MES', 'ZQ', 'TT', 'ZS', 'ZC', 'DX=F', 'VXM', 'CUS=F', 'SDA=F'],
            'multiplier': [5, 4167, 50000, 50, 50, 1000, 100, 250, 250]
        })
        pd.testing.assert_frame_equal(contract_multipliers, expected_df)


if __name__ == '__main__':
    unittest.main()