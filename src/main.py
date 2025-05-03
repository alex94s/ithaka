# -*- coding: utf-8 -*-
"""
Module Name: main.py

Description:
This is the main entry point for the application. It orchestrates the execution
of the Tracker, Factory and Strategy (BAM, CTA, EMM, NEWT, STAB, FAR) modules to ensure that the 
portfolio is rebalanced according to target allocations.

Author: Alex Schneider
Date: 2024-08-15
"""

import os
import webbrowser
import socket

import numpy as np
import pandas as pd
from dash import dash, dcc, html, Input, Output
import plotly.express as px
from plotly.graph_objects import Figure

import core.utils as ut
import core.tracker as tk

if __name__ == '__main__':
    ut.print_separator()
    run_mode = input("Enter run mode (live/test): ").strip().lower()
    ut.print_separator()
    while run_mode not in ['live', 'test']:
        print("Invalid run mode. Please enter 'live' or 'test'.")
        run_mode = input("Enter run mode (live/test): ").strip().lower()
    required_trades = tk.get_required_trades(
        epsilon=0.01, 
        run_mode=run_mode
    )
    app = dash.Dash(__name__)
    app.layout = html.Div([
        html.Div([
            dcc.DatePickerSingle(
                id='start-date-picker',
                date='2024-01-01',
                display_format='YYYY-MM-DD'
            )
        ], style={'margin-bottom': '20px'}),

        html.Div([
            html.Div([
                dcc.Graph(id='price-curve-graph')
            ], style={'display': 'inline-block', 'width': '50%'}),

            html.Div([
                html.H4('Statistics:'),
                html.Div(id='stats-output')
            ], style={'display': 'inline-block', 'width': '50%', 'padding-left': '2%'}),
        ], style={'display': 'flex', 'justify-content': 'space-between'}),

        html.Div([
            html.H4('Required Trades:'),
            html.Div(id='trades-output',
                 style={'display': 'inline-block', 'width': '45%'}),

            html.H4('Current Positions:'),
            html.Div(id='positions-output',
                 style={'display': 'inline-block', 'width': '45%', 'padding-left': '2%'}),
        ], style={'display': 'flex', 'justify-content': 'space-between', 'margin-top': '20px'})
    ])

    @app.callback(
        [Output('price-curve-graph', 'figure'),
         Output('stats-output', 'children'),
         Output('trades-output', 'children'),
         Output('positions-output', 'children')],
        Input('start-date-picker', 'date')
    )
    def update_tracer(start_date: str) -> tuple[
        Figure,
        dash.html.Ul,
        dash.html.Ul,
        dash.html.Ul
    ]:
        """
        Updates and generates performance statistics and plots for calculated strategies.

        Arguments:
            startDate: The start date for retrieving price data in 'YYYY-MM-DD' format.

        Returns:
            A tuple containing:
            - plotly.graph_objects.Figure: Line chart showing cumulative strategy returns.
            - dash.html.Ul: An HTML unordered list displaying output stats.
            - dash.html.Ul: An HTML unordered list displaying trade actions.
            - dash.html.Ul: An HTML unordered list displaying current positions.
        """
        conn, cursor = ut.connect_db()
        query = f"""
            SELECT * FROM price_data 
            WHERE (symbol LIKE '%BAM%' 
                    OR symbol LIKE '%CTA%' 
                    OR symbol LIKE '%EMM%'
                    OR symbol LIKE '%NEWT%'
                    OR symbol LIKE '%STAB%'
                    OR symbol LIKE '%FAR%'
                    OR symbol LIKE '%ITK%')
              AND date >= '{start_date}';
        """
        cursor.execute(query)

        query_result = pd.DataFrame(cursor.fetchall(), columns=[
            desc[0] for desc in cursor.description])

        df = query_result.pivot_table(
            index='date',
            columns='symbol',
            values='adj_close',
            aggfunc='sum'
        ).ffill().pct_change(fill_method=None)

        if 'NEWT' in df.columns:
            df['NEWT'].fillna(0, inplace=True)
        if 'ITK' in df.columns:
            df['ITK'].fillna(0, inplace=True)

        df = (1 + df.dropna()).cumprod()
        df = df.reset_index().melt(
            id_vars='date',
            var_name='symbol',
            value_name='cum_return'
        ).dropna()

        current_positions = tk.get_positions(cursor, 'portfolio')
        current_positions.columns = ['notional']

        stats = {}
        for symbol in df['symbol'].unique():
            subset = df[df['symbol'] == symbol]
            ann_return = (
                subset['cum_return'].iloc[-1] ** (
                    365 / (subset['date'].iloc[-1] -
                           subset['date'].iloc[0]).days)) - 1
            ann_vol = subset['cum_return'].pct_change(fill_method=None).std() * np.sqrt(252)
            sharpe_ratio = ann_return / ann_vol if ann_vol != 0 else np.nan
            stats[symbol] = {
                'Annualised Return': ann_return,
                'Annualised Volatility': ann_vol,
                'Sharpe Ratio': sharpe_ratio
            }
        fig = px.line(df, x='date', y='cum_return', color='symbol',
                      markers=False, title='ITK Dashboard')
        fig.update_layout(
            margin=dict(l=20, r=0, t=40, b=40),
            legend=dict(
                orientation="h",
                x=0, y=-0.2,
                traceorder="normal",
                font=dict(size=10),
                itemsizing="trace",
                itemclick="toggle",
                itemdoubleclick="toggleothers"
            )
        )
        stats_output = html.Ul([
            html.Li(
                f"{symbol}: AR={data['Annualised Return']:.2f}, "
                f"Vol={data['Annualised Volatility']:.2f}, "
                f"SR={data['Sharpe Ratio']:.2f}"
            )
            for symbol, data in stats.items()
        ])
        trades_output = html.Ul([
            html.Li(
                f"{row['action']} {row['notional']} USD "
                f"({round(row['quantity'], 2)} units) of {row['symbol']}"
            )
            for _, row in required_trades.iterrows()
        ])
        positions_output = html.Ul([
            html.Li(f"{index}: {str(row['notional'])}")
            for index, row in current_positions.iterrows()
            if row['notional'] != 0
        ])
        cursor.close()
        conn.close()
        return fig, stats_output, trades_output, positions_output

    def check_port_use(port: int) -> bool:
        """
        Checks if a specific port is in use on localhost.

        Arguments:
            port: The port number to check.

        Returns:
            True if the port is in use, False otherwise.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('127.0.0.1', port)) == 0

    def run_app() -> None:
        """
        Runs the Dash application.
        """
        port = 8050
        if check_port_use(port):
            webbrowser.open(f'http://127.0.0.1:{port}')
        else:
            webbrowser.open(f'http://127.0.0.1:{port}')
            app.run_server(debug=True, use_reloader=False)

    run_app()
    ut.print_separator()
    os.system("exit")
