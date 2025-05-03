# -*- coding: utf-8 -*-
"""
Module Name: test_factory.py

Description:
This module contains the unit tests for the Factory module. The unit tests in this 
module ensure the correctness and reliability of the functions utilised by using
mock objects to simulate database interactions and other dependencies.

Author: elreysausage
Date: 2024-10-30
"""

import unittest
from unittest.mock import patch, MagicMock

import pandas as pd

from core.factory import DataManager


class TestDataManager(unittest.TestCase):
    """
    Unit tests for the DataManager module.

    Attributes:
        data_manager: The DataManager object for testing.
        patcher_connect_ib: The patcher object for the connect_ib function.
        mock_ib: The mock IB object for testing.
    """
    @classmethod
    def setUp(cls):
        """
        Set up the test environment.

        Attributes:
            data_manager: The DataManager object for testing.
            patcher_connect_ib: The patcher object for the connect_ib function.
            mock_ib: The mock IB object for testing.
        """
        cls.patcher_connect_ib = patch('core.utils.connect_ib')
        mock_connect_ib = cls.patcher_connect_ib.start()
        cls.mock_ib = MagicMock()
        mock_connect_ib.return_value = cls.mock_ib
        cls.mock_ib.reqHistoricalData.return_value = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'close': [100, 101]
        })
        cls.data_manager = DataManager(run_mode='live')
        cls.data_manager.ticker_map = {'AAPL': MagicMock()}
    
    def test_download_price_data(self):
        """
        Test the download_price_data function.

        Asserts:
            The function returns the expected price data.
        """
        expected_data = pd.DataFrame(
            {'adj_close': [100, 101]}, 
            index=pd.to_datetime(['2023-01-02', '2023-01-03'])
        ).rename_axis('Date').asfreq('D')
        ticker_list = ['AAPL']
        price_data = self.data_manager.download_price_data(ticker_list)
        pd.testing.assert_frame_equal(price_data, expected_data)


if __name__ == '__main__':
    unittest.main()