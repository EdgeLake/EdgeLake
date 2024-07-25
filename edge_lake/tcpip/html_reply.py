

"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

'''
The URL is identified by:
output = html.json - print the JSON output of the data 
output = html.line - print a line graph
output = html.on_off - print On-Off buttons
output = html.bar - print a bar graph


Chart.js, supports various chart types using the type property:

line: A line chart is used to represent data points over time.
bar: A bar chart displays data with rectangular bars with lengths proportional to the values they represent.
radar: A radar chart displays data in a radial layout, with each data point represented as a vertex on a polygon.
doughnut: A doughnut chart is similar to a pie chart but with a hole in the center.
pie: A pie chart is a circular chart divided into slices to illustrate numerical proportions.
polarArea: A polar area chart is similar to a pie chart, but the radius of each segment is proportional to its value.
bubble: A bubble chart displays three dimensions of data, with the x and y values determining the position and the size of the bubble representing a third value.
scatter: A scatter chart displays data points as a collection of points, showing relationships between two variables.
area: An area chart is similar to a line chart but the area under the line is filled.
mixed: A mixed chart can combine different chart types in a single chart.
'''

# Transform AnyLog reply to HTML
# Example:
# http://10.0.0.78:7849/?User-Agent=AnyLog/1.23?command=get%20status%20where%20format%20=%20json?into=html
# http://10.0.0.78:7849/?User-Agent=AnyLog/1.23?into=html?destination=network?command=sql%20litsanleandro%20format=table%20and%20include=(percentagecpu_sensor)%20%22select%20increments(minute,%201,%20timestamp),%20device_name,%20min(timestamp)::ljust(19)::rjust(8)%20as%20timestamp,%20min(value)%20as%20min,%20max(value)%20as%20max,%20%20avg(value)::float(2)%20as%20avg,%20from%20ping_sensor%20where%20timestamp%20%3E=%20NOW()%20-%201hour%20GROUP%20BY%20device_name%20ORDER%20BY%20timestamp%20DESC%22
# http://10.0.0.78:7849/?User-Agent=AnyLog/1.23?into=html.on_off?destination=network?command=sql city_of_sabetha "select insert_timestamp, bg10_commsstatus from cos_pp where insert_timestamp <= now() order by insert_timestamp desc limit 3"
# http://10.0.0.78:7849/?User-Agent=AnyLog/1.23?into=html?destination=network?command=sql city_of_sabetha "select count(*) from cos_pp where insert_timestamp <= now()"
import sys
import json
import html
import re

html_default_style = {
                    ".center-title" : {
                        "display" : "flex;",
                        "flex-direction" : "column;",
                        "align-items" : "center;",
                        "justify-content" : "center;",
                        "height" : "height;",
                        "margin" : "0;",
                        "color" : "blue;",
                        "font-size" : "2em;",
                        "text-align" : "center;"
                        }
                    }

default_text_html_prefix = f"""
    <!DOCTYPE html>
    <html>
    <head>
          <meta charset="UTF-8">
          <title>AnyLog</title>
          <style>
            body {{
              background-color: black;
              color: white;
            }}
          </style>

    </head>
    <body>
        <p style="font-family: 'Courier New', Courier, monospace;">
"""

# For printout of the JSON data
default_json_html_prefix = f"""      
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>AnyLog</title>
          <style>
            body {{
              background-color: black;
              color: white;
              display: flex;
              justify-content: left;
              align-items: top;
              height: 100vh;
              margin: 0;
              font-family: Arial, sans-serif;
            }}
          </style>
        </head>
        <body>
            <p>
        """
default_html_suffix = f"""
            </p>
        </body>
        </html>
        """

backgroundColor_bar = [
    'rgba(255, 99, 132, 0.2)',
    'rgba(54, 162, 235, 0.2)',
    'rgba(255, 206, 86, 0.2)',
    'rgba(75, 192, 192, 0.2)',
    'rgba(153, 102, 255, 0.2)',
    'rgba(255, 159, 64, 0.2)',
    'rgba(201, 203, 207, 0.2)'
]
borderColor_bar = [
    'rgba(255, 99, 132, 1)',
    'rgba(54, 162, 235, 1)',
    'rgba(255, 206, 86, 1)',
    'rgba(75, 192, 192, 1)',
    'rgba(153, 102, 255, 1)',
    'rgba(255, 159, 64, 1)',
    'rgba(201, 203, 207, 1)'
]

# ------------------------------------------------------------------------
# Default line chart
# Example:
#  http://10.0.0.78:7849/?User-Agent=AnyLog/1.23?destination=network?into=html.line_chart?command=sql smart_city "SELECT increments(hour, 1, timestamp), max(timestamp) as timestamp , min(a_n_voltage), max(a_n_voltage), avg(a_n_voltage) from bf where timestamp >= now() - 1 day and timestamp <= now() and commsstatus is Null;"
# ------------------------------------------------------------------------
def get_chart_html(status, message, chart_type, added_html):

    counter_sets, data_set = set_chart_set(status, message, chart_type)
    if data_set:
        options = create_html_options( counter_sets, chart_type )    # chart.js options
        html_info =  create_html_chart(data_set, options, chart_type, added_html)
    else:
        html_info = default_html(f"Failed to deploy {chart_type} diagram - database output not in JSON format", False)

    return html_info

# ------------------------------------------------------------------------
# Default HTML reply for JSON data
# ------------------------------------------------------------------------
def json_html(status, json_string, chart_type, added_html):

    try:
        # Parse the JSON string into a Python object
        json_data = json.loads(json_string)

        # Pretty print the JSON data
        formatted_info = json.dumps(json_data, indent=4)
    except:
        formatted_info = json_string
        is_json = False
    else:
        is_json = True

    return default_html(formatted_info, is_json)

# ------------------------------------------------------------------------
# Default HTML in text - same size fonts
# ------------------------------------------------------------------------
def text_html(status, text_data, chart_type, added_html):

    return default_html(text_data, False)

# -----------------------------------------------------------------------
#  Create a Gauge
# -----------------------------------------------------------------------
def get_gauge_html(status, json_string, chart_type, added_html):

    entries = str_to_result_list(status, json_string, )
    if entries:
        html_info = get_gauge(entries, added_html)
    else:
        html_info = default_html(f"Failed to deploy Gauge diagram - database output not in JSON format", False)
    return html_info

# ------------------------------------------------------------------------
# Show the bool values in the HTML doc
# ------------------------------------------------------------------------
def get_on_off_html(status, json_string, chart_type, added_html):

    entries = str_to_result_list(status, json_string)
    if entries:
        html_info = on_off_html(entries, added_html)
    else:
        html_info = default_html(f"Failed to deploy the On Off buttons - database output not in JSON format", False)
    return html_info



chart_types_ = {
                # User selection        Type to use
                  'line' :              ["line", get_chart_html],
                  'bar' :               ["bar", get_chart_html],
                  'multiscale' :        ["bar", get_chart_html],    # multiscale treated like "bar"
                  'radar' :             ["radar", get_chart_html],
                  'doughnut' :          ["doughnut", get_chart_html],
                  'pie' :               ["pie", get_chart_html],
                  'polararea' :         ["polararea", get_chart_html],
                  'bubble' :            ["bubble", get_chart_html],
                  'scatter' :           ["scatter", get_chart_html],
                  'area' :              ["area", get_chart_html],
                  'json':               ["json", json_html],
                  'gauge':              ["gauge", get_gauge_html],
                  'onoff':              ["onoff", get_on_off_html],
                  'text':               ["text", text_html]
}

# ------------------------------------------------------------------------
# Map to HTML Reply
# ------------------------------------------------------------------------

def to_html( status, message, source_format, type_html, html_info):
    '''
    status - AnyLog object
    message - the output to deliver
    source_format - text/json or text
    type_html - html, or html with policy
    '''


    if html_info:
        # additional HTML info
        if "html" in html_info:
            added_html = html_info["html"]      # get the inner of the HTML policy
        else:
            added_html = html_info              # Special instructions through the user JSON box
    else:
        added_html = None

    if type_html[:5] == "html." and len(type_html) > 5 and type_html[5:] in chart_types_:
        html_reply = chart_types_[type_html[5:]][1](status, message, type_html[5:], added_html)
    else:
        html_reply = json_html(status, message, "json", added_html)

    return html_reply


# ------------------------------------------------------------------------
# Default HTML reply
# ------------------------------------------------------------------------
def default_html(message, is_json):
    '''
    message - the message to return
    is_json = True for JSON format
    '''

    html_msg = str_to_html(message)

    if is_json:
        prefix_html = default_json_html_prefix
    else:
        prefix_html = default_text_html_prefix

    return prefix_html + html_msg + default_html_suffix

# ------------------------------------------------------------------------
# Default HTML for buttons
'''
Example entries
entries = [
    {"name": "John", "value": True},
    {"name": "Alice", "value": False},
    {"name": "Bob", "value": True},
    {"name": "Eve", "value": False}
]
'''
# ------------------------------------------------------------------------
def on_off_html(entries, added_html):
    # Start building the HTML string
    html_code = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    """

    html_code += """
    <style>
        /* Some basic styling for demonstration */
        button {
            margin: 5px;
        }
        .status-box {
            width: 200px;
            height: 200px;
            text-align: center;
            border: 1px solid black;
            border-radius: 8px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            margin: 5px;
        }
        .status-on {
            background-color: darkseagreen;
            color: white;
        }
        .status-off {
            background-color: indianred;
            color: white;
        }
    </style>
    """
    if added_html:
        # Hook
        # Add user instructions for the head
        html_code += html_from_json(added_html, "head", None)

    html_code += """
    
    <title>AnyLog</title>
    </head>
    <body>
    """
    if added_html:
        # Hook
        # Add user instructions for the title
        html_code += html_from_json(added_html, "body", "top")

    html_code += """

    <div style="display: flex; flex-wrap: wrap; gap: 20px; justify-content: center;">

    """

    # Loop through entries and dynamically add HTML for each entry
    for entry in entries:
        status_class = "status-on" if entry['value'] else "status-off"
        html_code += f"""
        <div class="status-box {status_class}">
            <div style="line-height: 1; font-weight: bold;">{"ON" if entry['value'] else "OFF"}</div>
            <div style="line-height: 1;">{entry['name']}</div>
        </div>
        """

    # Complete the HTML structure
    html_code += """
    </div>

    </body>
    </html>
    """

    return html_code

# ------------------------------------------------------------------------
# Default line chart
# Example:
#  http://10.0.0.78:7849/?User-Agent=AnyLog/1.23?destination=network?into=html.line_chart?command=sql smart_city "SELECT increments(hour, 1, timestamp), max(timestamp) as timestamp , min(a_n_voltage), max(a_n_voltage), avg(a_n_voltage) from bf where timestamp >= now() - 1 day and timestamp <= now() and commsstatus is Null;"
# ------------------------------------------------------------------------
def create_html_chart(data_set, options, chart_type, added_html):
    html_code = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        
        
        
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    """
    if added_html:
        # Hook
        # Add user instructions for the head
        html_code += html_from_json(added_html, "head", None)


    html_code += """
    <title>AnyLog</title>    
    </head>
    <body>
    """

    if added_html:
        # Hook
        # Add user instructions for the title
        html_code += html_from_json(added_html, "body", "top")


    html_code += """    
        <div style="width: 50%; margin: auto;">
            <canvas id="myMultiLineChart"></canvas>
        </div>
        <script>
            const ctx = document.getElementById('myMultiLineChart').getContext('2d');
            const myMultiLineChart = new Chart(ctx, {
    """
    # Add CHART TYPE
    chart_to_use = chart_types_[chart_type][0]  # For example, multiscale is treated like bar
    html_code += f"type: '{chart_to_use}',\n"     # Specify the chart type as 'line', 'bar', 'radar', 'doughnut', 'pie', 'polarArea', 'bubble', 'scatter', 'area', 'mixed'

    # Add the data setts and colors
    html_code += data_set

    html_code += options

    html_code += """
                });
        </script>
    </body>
    </html>
    """

    return html_code

# ------------------------------------------------------------------------
# String to HTML
# ------------------------------------------------------------------------
def str_to_html(message):
    # First, escape any special HTML characters
    text = html.escape(message)

    # Use re.sub with a function to replace \n, spaces, and \r in one traversal
    html_msg = re.sub(r'[\n\r ]', replacement_function, text)

    return html_msg

# ------------------------------------------------------------------------
# Replacement function for re (from str to html)
# ------------------------------------------------------------------------
def replacement_function(match):
    if match.group() == '\n':
        return '<br>'
    elif match.group() == ' ':
        return '&nbsp;'
    elif match.group() == '\r':
        return ''
    return match.group()

# ------------------------------------------------------------------------
# Mapping the JSON structure to a list:
'''
{'Query': [
    {
        'insert_timestamp' = {str} '2024-06-13 11:44:44.656168'
        'bg10_commsstatus' = {bool} True
    },
    {    
        'insert_timestamp' = {str} '2024-06-13 11:44:44.656168'
        'bg10_commsstatus' = {bool} True
    },
    {    
        'insert_timestamp' = {str} '2024-06-13 11:44:44.656168'
        'bg10_commsstatus' = {bool} False
    }
]
   
to:

Example entries
entries = [
    {"key": "bg10_commsstatus", "value": 1},
    {"key": "bg10_commsstatus", "value": 1},
    {"key": "bg10_commsstatus", "value": 0}
]
'''


# ------------------------------------------------------------------------
def str_to_result_list(status, json_string):

    out_list = None
    try:
        # Parse the JSON string into a Python object
        json_data = json.loads(json_string)
    except:
        pass
    else:
        if "Query" in json_data:
            entries_list = json_data["Query"]
            # Validate all attr are numbers
            if isinstance(entries_list, list) and len(entries_list):
                out_list = []
                for entry in entries_list:  # Every entry is a dict in the query result
                    for index, key_val in enumerate(entry.items()):
                        if index:
                            key = key_val[0]
                            val = key_val[1]
                            # Not the time column
                            if isinstance(val, bool) or isinstance(val, str):
                                value = 1 if val else 0
                            else:
                                value = val  # In t or float
                            out_list.append({"name": key, "value": value})

    return out_list       # Return a list of attribute name value


# ------------------------------------------------------------------------
# Mapping the JSON structure to :
'''

'{"Query": [{"insert_timestamp": "2024-06-25 19:46:13.304291", 
            "a_n_voltage": 732, 
            "b_n_voltage": 736, 
            "c_n_voltage": 733}],


to:
Labels        ["a_n_voltage", "b_n_voltage", "c_n_voltage"]
Data          [732, 736, 733]

'''


# ------------------------------------------------------------------------
def str_to_labels_and_data(status, json_string):
    lables = []
    data = []
    try:
        # Parse the JSON string into a Python object
        json_data = json.loads(json_string)
    except:
        pass
    else:
        if "Query" in json_data:
            entries_list = json_data["Query"]
            # Validate all attr are numbers
            if isinstance(entries_list, list) and len(entries_list):
                for entry in entries_list:  # Every entry is a dict in the query result
                    for index, key_val in enumerate(entry.items()):
                        if index:
                            key = key_val[0]
                            val = key_val[1]
                            lables.append(key)
                            data.append(val)

    return [lables,data]  # Return a list of attribute name value


# ------------------------------------------------------------------------

'''
    Example of an entry:
        {
            'timestamp' = {str} '2024-06-25 08:59:40.961578'
            'min(a_n_voltage)' = {int} 728
            'max(a_n_voltage)' = {int} 733
            'avg(a_n_voltage)' = {float} 730.6666666666666
        }   
    
    set_chart_set - transforms the data to this format:
    
                data: {
                    labels: ['January', 'February', 'March', 'April', 'May', 'June', 'July'],
                    datasets: [
                        {
                            label: 'Sales 2023',
                            data: [65, 59, 80, 81, 56, 55, 40],
                            backgroundColor: 'rgba(75, 192, 192, 0.2)', // Area under the line
                            borderColor: 'rgba(75, 192, 192, 1)', // Line color
                            borderWidth: 1
                        },
                        {
                            label: 'Sales 2024',
                            data: [75, 69, 90, 91, 66, 65, 50],
                            backgroundColor: 'rgba(255, 99, 132, 0.2)', // Area under the line
                            borderColor: 'rgba(255, 99, 132, 1)', // Line color
                            borderWidth: 1
                        }
                    ]
                },

'''
# ------------------------------------------------------------------------
def set_chart_set(status, json_string, chart_type):

    data_set = None
    counter_sets = 0
    try:
        # Parse the JSON string into a Python object
        json_data = json.loads(json_string)
    except:
        pass
    else:
        if "Query" in json_data:
            entries_list = json_data["Query"]
            if len(entries_list):
                counter_sets = len(entries_list[0]) - 1     # Look at the first entry to determine the number of sets. The first attr_val determine the labels
                if counter_sets >= 0:


                    data = {
                        "labels" : [],
                        "datasets" : []
                    }

                    # Loop to create unique dataset dictionaries
                    for i in range(counter_sets):
                        info_set = {
                            "label": f"'Dataset {i + 1}'",
                            "data": [],
                            "backgroundColor": "",
                            "borderColor": "",
                            "borderWidth": 1
                        }
                        if chart_type == "multiscale":
                            info_set["yAxisID"] = f"'y-axis-{i + 1}'"
                        data["datasets"].append(info_set)


                    for entry_counter, entry in enumerate(entries_list):
                        colors_index = 0
                        for index, key_val in enumerate(entry.items()):
                            key = key_val[0]
                            val = key_val[1]

                            if not index:
                                    # Get the X axes values
                                    data["labels"].append(f"'{val}'")
                            else:
                                info_id = index - 1
                                if entry_counter == 0:
                                    # with the first Entry
                                    data["datasets"][info_id]["label"] = f"'{key}'"
                                    data["datasets"][info_id]["backgroundColor"] = f"'{backgroundColor_bar[colors_index]}'"
                                    data["datasets"][info_id]["borderColor"] = f"'{borderColor_bar[colors_index]}'"
                                    colors_index += 1
                                    if colors_index >= len(backgroundColor_bar):
                                        index = 0       # Reuse colors

                                data["datasets"][info_id]["data"].append(val)

                    try:
                        # Parse the JSON string into a Python object

                        data_set = json.dumps(data, indent=4)
                    except:
                        pass
                    else:
                        data_set = "data: \n" + data_set.replace("\"","") + ",\n"

    return [counter_sets, data_set]

# -----------------------------------------------------------------------
#  Create the chart.js options part-
# -----------------------------------------------------------------------
def create_html_options( counter_sets, chart_type ):
    if chart_type == "multiscale":
        html_options = get_multiscale_options(counter_sets)
    else:
        html_options = get_default_options()

    return html_options

# -----------------------------------------------------------------------
#  Create the chart.js options for multi scale
#
'''
    options: {
            scales: {
                yAxes: [
                    {
                        id: 'y-axis-1',
                        type: 'linear',
                        position: 'left',
                        ticks: {
                            beginAtZero: true
                        }
                    },
                    {
                        id: 'y-axis-2',
                        type: 'linear',
                        position: 'right',
                        ticks: {
                            beginAtZero: true
                        },
                        gridLines: {
                            drawOnChartArea: false
                        }
                    }
                ]
            }
        }
    '''
# -----------------------------------------------------------------------
def get_multiscale_options(counter_sets):

    options = {
        "scales" : {
            "yAxes" : [

            ]

        }
    }

    for set_id in range (counter_sets):
        # Set the per set options info
        option_info = {
            "id": f"'y-axis-{set_id + 1}'",
            "type": "'linear'",
            "position": "'left'",
            "ticks": {
                "beginAtZero": True
            }

        }
        if set_id:
            # Not with the first entry
            option_info["gridLines"] = {
                            "drawOnChartArea": False
                        }
            option_info["offset"] = True

        options["scales"]["yAxes"].append(option_info)


    try:
        # Parse the JSON string into a Python object

        options = json.dumps(options, indent=4)
    except:
        # Failed
        options = get_default_options()
    else:
        options = "options: \n" + options.replace("\"","")

    return options


# -----------------------------------------------------------------------
#  Create the default chart.js options
# -----------------------------------------------------------------------
def get_default_options():
    html_options = """ 
    options: {
        scales: {
            y: {
                beginAtZero: true
            }
        }
    }
    """
    return html_options

# -----------------------------------------------------------------------
#  Create a Gauge
# -----------------------------------------------------------------------
def get_gauge(entries, added_html):

    html_options = """ 
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <!-- Include Plotly.js library -->
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    """

    if added_html:
        # Hook
        # Add user instructions for the head
        html_options += html_from_json(added_html, "head", None)


    html_options += """
    <title>AnyLog</title>  
    </head>
    <body>
    """

    if added_html:
        # Hook
        # Add user instructions for the title
        html_options += html_from_json(added_html, "body", "top")

    for index in range (len(entries)):
        html_options += f"\n<div id=\"gaugeChart{index + 1}\" style=\"width: 400px; height: 400px; display: inline-block;\"></div>"

    html_options += """
        "\n""   
        <script>
            // Data for the first gauge chart
    """

    html_options += get_gauge_info(entries, added_html)

    for index in range(len(entries)):
        #  Layout configuration for the gauge charts
        html_options += f"\nvar layout{index + 1} = {{ width: 400, height: 400, margin: {{ t: 0, b: 0 }} }};"

    for index in range(len(entries)):
        #  Plotting the gauge charts
        html_options += f"\nPlotly.newPlot('gaugeChart{index + 1}', data{index + 1}, layout{index + 1});"

    html_options += """\n
        </script>
    </body>
    </html>

    """
    return html_options

# -----------------------------------------------------------------------
#  Get the gauge info:
'''

            var data1 = [
                {
                    type: 'indicator',
                    mode: 'gauge+number',
                    value: 65,
                    title: { text: "Gauge 1" },
                    gauge: {
                        axis: { range: [null, 100] },
                        steps: [
                            { range: [0, 50], color: "lightgray" },
                            { range: [50, 80], color: "gray" }
                        ],
                        threshold: {
                            line: { color: "red", width: 4 },
                            thickness: 0.75,
                            value: 90
                        }
                    }
                }
            ];
    
            // Data for the second gauge chart
            var data2 = [
                {
                    type: 'indicator',
                    mode: 'gauge+number',
                    value: 35,
                    title: { text: "Gauge 2" },
                    gauge: {
                        axis: { range: [null, 100] },
                        steps: [
                            { range: [0, 50], color: "lightblue" },
                            { range: [50, 80], color: "blue" }
                        ],
                        threshold: {
                            line: { color: "orange", width: 4 },
                            thickness: 0.75,
                            value: 60
                        }
                    }
                }
            ];

'''
'''
Update the Gauge with the following HTML Info:

{ "data" :
            { 
            "max": {
                            "axis": {"range": [None, 100]},
                            "steps": [
                                {"range": [0, 30], "color": "blue"},
                                {"range": [30, 80], "color": "yellow"},
                                {"range": [80, 100], "color": "orange"}
                            ],
                            "threshold": {
                                "line": {"color": "red", "width": 4},
                                "thickness": 0.75,
                                "value": 90
                            }
                    },
            "*": {
                            "axis": {"range": [None, 100]},
                            "steps": [
                                {"range": [0, 50], "color": "lightgray"},
                                {"range": [50, 80], "color": "gray"}
                            ],
                            "threshold": {
                                "line": {"color": "red", "width": 4},
                                "thickness": 0.75,
                                "value": 90
                            }
                        
                }
            }
}
'''
# -----------------------------------------------------------------------
def get_gauge_info(entries, added_html):

    html_info = ""

    for index, entry in enumerate(entries):
        vardata = [
            {
                "type" : "'indicator'",
                "mode" : "'gauge+number'",         # displays the gauge chart with a dial  + the numerical value
                "value": 65,
                "title": {"text": "Gauge 1"},
                "gauge": {
                    "axis": {"range": [None, 100]},
                    "steps": [
                        {"range": [0, 50], "color": "'lightgray'"},
                        {"range": [50, 80], "color": "'gray'"}
                    ],
                    "threshold": {
                        "line": {"color": "'red'", "width": 4},
                        "thickness": 0.75,
                        "value": 90
                    }
                }
            }
        ]

        value = entry["value"]
        if not isinstance(value, int) and not isinstance(value, float):
            try:
                value = int(entry["value"])
            except:
                value = 0

        test_value = abs(value)
        if test_value < 1:
            nearest_power =  1 if value > 0 else -1  # Assuming 1 is the nearest power of 10 for numbers <= 0
        else:
            num_digits = len(str(int(test_value)))  # Counting the digits of the integer part
            nearest_power = 10 ** num_digits
            if value < 0:
                nearest_power = -nearest_power

        vardata[0]["value"] = value
        gauge_name = entry['name']
        vardata[0]["title"]["text"] = f"'{gauge_name}'"
        gauge_info = vardata[0]["gauge"]
        # Default values
        gauge_info["axis"]["range"][1] = nearest_power
        gauge_info["steps"][0]["range"][1] = nearest_power / 2
        gauge_info["steps"][1]["range"][0] = nearest_power / 2
        gauge_info["steps"][1]["range"][1] = nearest_power

        if added_html:
            if "data" in added_html:
                data_updates = added_html["data"]    # Update the data part
                if gauge_name in data_updates:
                    added_info = data_updates[gauge_name]
                elif '*' in data_updates:
                    # All the rest
                    added_info = data_updates['*']
                else:
                    added_info = None
                if added_info:
                    if isinstance(added_info, dict):
                        # User provided info to the gauge
                        for key, value in added_info.items():
                            gauge_info[key] = convert_strings_to_single_quotes(value)   # GO over the structure and fix string value sto be with a sinle quotation

        try:
            # Parse the JSON string into a Python object

            vardata_dumps = json.dumps(vardata, indent=4)
        except:
            # Failed
            html_info = ""
            break
        else:

            html_info += f"var data{index+1} = \n" + vardata_dumps.replace("\"", "") + ';\n'

    return html_info

# -----------------------------------------------------------------------------
# Get the user info for the HTML page
# This is the info provided in the TEXT/JSON input box
# -----------------------------------------------------------------------------
def url_to_json(status, html_info):

    json_string = html_info.replace('^', r'"')
    json_string = json_string.replace("None", "null")
    try:
        # Parse the JSON string into a Python object
        added_html = json.loads(json_string)
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = f"Failed to parse user provided html info in json: '{value}' Using: '{json_string}'"
        status.add_error(err_msg)
        added_html = None
    else:
        err_msg = ""

    return [added_html, err_msg]

# -----------------------------------------------------------------------------
# Add a single quotation to a string
# -----------------------------------------------------------------------------
def convert_strings_to_single_quotes(obj):
    # GO over the structure and fix string value sto be with a sinle quotation
    if isinstance(obj, dict):
        return {k: convert_strings_to_single_quotes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_strings_to_single_quotes(item) for item in obj]
    elif isinstance(obj, str):
        return f"'{obj}'"
    else:
        return obj

# -----------------------------------------------------------------------------
# Add user instructions to the html section
'''
{ "head" : {
            "include" : ["<title>AnyLog</title>"],
            "style" : {
                    ".center-title" : {
                        "display" : "flex;",
                        "flex-direction" : "column;",
                        "align-items" : "center;",
                        "justify-content" : "center;",
                        "height" : "height;",
                        "margin" : "0;",
                        "color" : "blue;",
                        "font-size" : "2em;",
                        "text-align" : "center;"
                        }
                    }
            },
 "body" : { 
            "top" : 
                {"include" : [
                            "<br><br>",
                            "<div class='center-title'>Center Title Text</div>",
                            "<br>"
                            ]
                }
            } 
 }              
'''

'''
Simplified:

{ "head" : {
            "style" : {
                    "default" : {
                                "color" : "green"
                                }
                    }
            },
 "body" : { 
            "top" : 
                {"include" : [
                            "<br><br>",
                            "<div class='center-title'>Test Topic</div>",
                            "<br>"
                            ]
                }
            } 
 } 
    
  
'''
# -----------------------------------------------------------------------------
def html_from_json(added_html, html_section, subsection):
    '''
    added_html - the user instructions
    html_section - head or body
    subsection - for example: top --> body + top vs. body + bottom
    '''

    new_section = ""
    if html_section in added_html:

        if subsection:
            if subsection in added_html[html_section]:
                instructions = added_html[html_section][subsection]
            else:
                instructions = None
        else:
            instructions = added_html[html_section]

        if instructions and isinstance(instructions,dict):
            for key, val in instructions.items():
                if key == "include":            # ADd the include instructions as is
                    if isinstance(val, list):
                        for entry in val:
                            new_section += f"{entry}\n"
                elif key == "style":
                    new_section += "<style>\n"
                    if isinstance(val,dict):

                        if "default" in val:
                            # Modify default values
                            for property, value in html_default_style.items():
                                new_section += f"{property} "
                                if isinstance(value, dict):
                                    # Note: no quotations and commas
                                    new_section += "{\n"
                                    for property_key, property_val in value.items():
                                        if property_key in val["default"]:
                                            # Take user defs
                                            user_value = str(val['default'][property_key])
                                            if len(user_value):
                                                if user_value[-1] != ';':
                                                    user_value += ';'
                                                new_section += f"\n{property_key}: {user_value}"
                                        else:
                                            # Take default value
                                            new_section += f"\n{property_key}: {property_val}"
                                    new_section += "}\n"

                        else:
                            # use user defs
                            for property, value in val.items():
                                new_section += f"{property} "
                                if isinstance(value, dict):
                                    # Note: no quotations and commas
                                    new_section += "{\n"
                                    for property_key, property_val in value.items():
                                        new_section += f"\n{property_key}: {property_val}"
                                    new_section += "}\n"

                    new_section += "</style>\n"
    return new_section








