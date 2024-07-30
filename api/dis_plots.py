''' dis_plots.py
    Plot functions for the DIS UI
'''

from math import pi
import pandas as pd
from bokeh.embed import components
from bokeh.palettes import plasma, Viridis
from bokeh.plotting import figure
from bokeh.transform import cumsum

SOURCE_PALETTE = ["mediumblue", "darkorange"]
SOURCE3_PALETTE = ["mediumblue", "darkorange", "wheat"]
TYPE_PALETTE = ["mediumblue", "darkorange", "wheat", "darkgray"]

def pie_chart(data, title, legend, colors=None):
    ''' Create a pie chart
        Keyword arguments:
          data: dictionary of data
          title: chart title
          legend: data key name
          colors: list of colors (optional)
        Returns:
          Figure components
    '''
    if not colors:
        colors = Viridis[len(legend) + 1]
    pdata = pd.Series(data).reset_index(name='value').rename(columns={'index': legend})
    pdata['angle'] = pdata['value']/pdata['value'].sum() * 2*pi
    pdata['color'] = colors
    plt = figure(title=title, toolbar_location=None,
                 tools="hover", tooltips=f"@{legend}: @value", x_range=(-0.5, 1.0))
    plt.wedge(x=0, y=1, radius=0.4,
              start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
              line_color="white", fill_color='color', legend_field=legend, source=pdata)
    plt.axis.axis_label = None
    plt.axis.visible = False
    plt.grid.grid_line_color = None
    return components(plt)


def stacked_bar_chart(data, title, xaxis, yaxis, colors=None):
    ''' Create a stacked bar chart
        Keyword arguments:
          data: dictionary of data
          title: chart title
          xaxis: x-axis column name
          yaxis: list of y-axis column names
          colors: list of colors (optional)
        Returns:
          Figure components
    '''
    if not colors:
        colors = plasma(len(yaxis))
    plt = figure(x_range=data[xaxis], title=title,
                 toolbar_location=None, tools="hover",
                 tooltips=f"$name @{xaxis}: @$name")
    plt.vbar_stack(yaxis, x=xaxis, width=0.9,
                   color=colors, source=data,
                   legend_label=yaxis
                   )
    plt.legend.location = 'top_left'
    plt.xgrid.grid_line_color = None
    plt.y_range.start = 0
    plt.background_fill_color = "ghostwhite"
    return components(plt)
