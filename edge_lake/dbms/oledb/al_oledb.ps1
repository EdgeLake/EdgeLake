<#
Script to verify that a provider/driver works.

Parameters:
   connectionString: connection string to connect to the data source. Optional.
                     If not provided, wizard will be started to create connection string.
   query           : query to be executed. Optional.
                     If not provided, default query for the data source is selected from the configuration file.
   help            : Usage is printed out.
#>

<#
.\iPisql.ps1 -connectionString 'Provider=PISQLClient;Data Source=18.217.99.117\anylog; User ID=Jun.Zha;Password=Nan62112!@;' -query 'SELECT TOP 100 Path, Name, Level, ElementID, IsPrimaryPath FROM [Master].[Element].[ElementHierarchy] WHERE Level = 1'
$connectionString = 'Provider=PISQLClient;Data Source=18.217.99.117\anylog; User ID=Jun.Zha;Password=Nan62112!@;'
$query = 'SELECT TOP 100 Path, Name, Level, ElementID, IsPrimaryPath FROM [Master].[Element].[ElementHierarchy] WHERE Level = 1'

Notes:
 Install-Module ThreadJob -Scope CurrentUser -Force
 cd d:\AnyLog-Code\AnyLog-Network\source\dbms\oledb
#>

param (
   [Parameter(Mandatory = $false)]
   [string] $connectionString,
   [Parameter(Mandatory = $false)]
   [string] $query,
   [switch] $help
)

function Write-SeparatorLine {
   Write-Host ("-" * 40)
}

function Write-Message {
   Write-Host
   Write-Host -ForegroundColor $args[1] -BackgroundColor $args[2] $args[0]
   Write-Host
}

function Write-Error {
   Write-Message $args[0] $Host.PrivateData.ErrorForegroundColor $Host.PrivateData.ErrorBackgroundColor
}

function Write-Warning {
   Write-Message $args[0] $Host.PrivateData.WarningForegroundColor $Host.PrivateData.WarningBackgroundColor
}

function ScriptExit {
   Write-Host "Terminating AnyLog OLEDB connector"
   exit
}

# -----------------------------------------------------------------------------
# Push data back to the REST caller - input data is string
# -----------------------------------------------------------------------------
function push_data() {
#$Response, $buffer
    param (
        [Parameter(Mandatory = $true)]
        [object] $Response,
        [Parameter(Mandatory = $true)]
        [string] $buffer
    )

    $bytes = [System.Text.Encoding]::Unicode.GetBytes($buffer)

    push_data_bytes $Response $bytes
}

# -----------------------------------------------------------------------------
# Push data back to the REST caller - input data is bytes array
# -----------------------------------------------------------------------------
function push_data_bytes() {

    param (
        [Parameter(Mandatory = $true)]
        [object] $Response,
        [Parameter(Mandatory = $true)]
        [byte[]] $buffer
    )

    try{
        $Response.ContentLength64 = $buffer.length
        $Response.OutputStream.Write($buffer, 0, $buffer.length)
    }catch
    {
       Write-Host $_.Exception.Message
       $data_rows = "{""status"":""Exception error when pulling data from server""}"
    }


}
# -----------------------------------------------------------------------------
# Connect to data source
# -----------------------------------------------------------------------------
function connect_to_dbms {


    param (
        [Parameter(Mandatory = $true)]
        [object] $logical_dbms,
        [Parameter(Mandatory = $true)]
        [object] $connect_str
    )

     $ret_val = $false

    if (!$logical_dbms){
         Write-Host "`nNot able to connect - missing dbms name"
    }elseif (!$connect_str){
         Write-Host "`nNot able to connect - missing connect string"
    }else{

        # try to connect

        $provider = "System.Data.OleDb.OleDbConnection"

        $connection = New-Object $provider

        

        try {
            # Set the connection string, open a connection to the data source, and execute the query
            $connection.ConnectionString = $connect_str
            $connection.Open()

            $connection_pool[$logical_dbms] = $connection     # Save the connection as f(DBMS NAME)

            Write-Host "`nConnected to the PI SQL Data Access Server version $($connection.ServerVersion)."
            $ret_val = $true
        }catch{
           Write-Host $_.Exception.Message
          
        }
    }

    return $ret_val
}

# -----------------------------------------------------------------------------
# Process Job using a dedicated thread
# -----------------------------------------------------------------------------
$process_query_job = {

        param (
            [Parameter(Mandatory = $true)]
            [object] $dbms_connect,
            [Parameter(Mandatory = $true)]
            [object] $query_texts,
            [Parameter(Mandatory = $true)]
            [object] $context
        )




        # -----------------------------------------------------------------------------
        # Push data back to the REST caller - input data is string
        # -----------------------------------------------------------------------------
        function push_data() {

            param (
                [Parameter(Mandatory = $true)]
                [object] $Response,
                [Parameter(Mandatory = $true)]
                [string] $buffer
            )

            $bytes = [System.Text.Encoding]::Unicode.GetBytes($buffer)

            push_data_bytes $Response $bytes
        }


        # -----------------------------------------------------------------------------
        # Push data back to the REST caller - input data is bytes array
        # -----------------------------------------------------------------------------
        function push_data_bytes() {

            param (
                [Parameter(Mandatory = $true)]
                [object] $Response,
                [Parameter(Mandatory = $true)]
                [byte[]] $buffer
            )

            try{
                $Response.ContentLength64 = $buffer.length
                $Response.OutputStream.Write($buffer, 0, $buffer.length)
            }catch
            {
               Write-Host $_.Exception.Message
               $data_rows = "{""status"":""Exception error when pulling data from server""}"
            }

        }


        # -----------------------------------------------------------------------------
        # Print formatted columns
        # -----------------------------------------------------------------------------
        function get_formatted_columns ($Reader) {

            $columnNames = New-Object System.Collections.Generic.List[System.String]

            for ($i = 0; $i -lt $reader.FieldCount; $i++) {

                $columnName = $reader.GetName($i)

                # If the column does not have a name, call them "Column<columnNumber>"
                if (!$columnName) {
                    $columnName = "Column" + $i
                }

                # Make sure the column name is unique
                while ($columnNames.Contains($columnName)) {
                    $columnName += '_'
                }

                $columnNames.Add($columnName);
            }



            return $columnNames

        }
        # -----------------------------------------------------------------------------
        # Print formatted rows
        # -----------------------------------------------------------------------------
        function print_formatted_rows (){

            param (
                [Parameter(Mandatory = $true)]
                [object] $reader,
                [Parameter(Mandatory = $true)]
                [object] $columnNames
            )


            $rows = New-Object System.Collections.Generic.List[PSObject]
            $hasRows = $false

            while ($reader.read()) {

                $hasRows = $true
                # Read the data
                $row = New-Object PSObject
                for ($i = 0; $i -lt $reader.FieldCount; $i++) {
                    $row | Add-Member NoteProperty $columnNames[$i]($reader.GetValue($i).ToString()) 
                }
                $rows.Add($row)

                if ($rows.Count -eq 20) {
                    # Print the partial result
                    $rows | Format-Table -Wrap
                    $rows.Clear()
                }
            }

            # Print the partial result
            if ($rows.Count -gt 0) {
                $rows | Format-Table -Wrap
            }

            if (!$hasRows) {
                Write-Warning "No rows found."
            }

        }

        # -----------------------------------------------------------------------------
        # Print JSON rows
        # -----------------------------------------------------------------------------
        function get_json_rows {

            param (
                [Parameter(Mandatory = $true)]
                [object] $reader,
                [Parameter(Mandatory = $true)]
                [object] $columnNames,
                [Parameter(Mandatory = $true)]
                [object] $Response
            )


            $all_rows = New-Object System.Collections.Generic.List[PSObject]
            $hasRows = $false
            
            try
            {
 

                while ($reader.read()) {

                    $hasRows = $true
                    # Read the data
                    $row = New-Object PSObject   # PSObject uses a key & value pair type structure. To store data

                    for ($i = 0; $i -lt $reader.FieldCount; $i++) {
                        $row | Add-Member NoteProperty $columnNames[$i]($reader.GetValue($i).ToString()) 
                    }

                    $all_rows.Add($row)
        
                }

            }catch{
                Write-Host $_.Exception.Message
          
            }



            if (!$hasRows) {
                $str_data = ""

            }else{
                $str_data = $all_rows | ConvertTo-Json
            }

            push_data $Response $str_data

        }


        # -----------------------------------------------------------------------------
        # Pull data from the DBMS and Push data to the Caller
        # -----------------------------------------------------------------------------
        function get_from_dbms {

            # param (  $dbms_connect, $query_texts, $Response, ${Function:get_columns},  ${Function:get_rows} )


            param (
                [Parameter(Mandatory = $true)]
                [object] $dbms_connect,
                [Parameter(Mandatory = $true)]
                [object] $query_texts,
                [Parameter(Mandatory = $true)]
                [object] $context
            )

            $Response = $context.Response

            $data_rows = "{""status"":""Empty data set""}"

            try
            {
                $Response.statuscode = 200

                #Write-Host "`nConnected to the PI SQL Data Access Server version $($connection.ServerVersion)."

                $command = $dbms_connect.CreateCommand() 
                $command.CommandTimeout = 600      # set timeout to 10 minutes
                try {
                    # $command.CommandText = "SELECT TOP 10 eh.Name Element, ea.Name Attribute, a.Time, a.Value FROM [AMI6].[Asset].[ElementHierarchy] eh INNER JOIN [AMI6].[Asset].[ElementAttribute] ea ON ea.ElementID = eh.ElementID INNER JOIN [AMI6].[Data].[Archive] a ON a.ElementAttributeID = ea.ID WHERE eh.Path = N'\' AND ea.Name = 'Relative Humidity' AND a.Time BETWEEN N'*-1Y' AND N'*'"
                    $command.CommandText = $query_texts # $query

                    $reader = $command.ExecuteReader()
                    try {

                    $columnNames = get_formatted_columns $reader     # Print the column names

                    #print_formatted_rows $reader $columnNames          # Print the data rows

                    get_json_rows $reader $columnNames $Response
 
 
                    }
                    finally {
                    $reader.Dispose()
                    }
                }
                finally {
                    $command.Dispose()
                }
  
            }
            catch
            {
       
               Write-Host "Failed to process command on server: $_.Exception.Message"
               Write-Host "--> $query"

               $data_rows = "{""status"":""Exception error when pulling data from server""}"
               $Response.statuscode = 501   # Server failed to satisfy the request
               push_data $Response $data_rows

            }

            $Response.Close()

        }

   

    get_from_dbms $dbms_connect $query_texts $context
    


}


# -----------------------------------------------------------------------------
# Start the REST SERVER
# -----------------------------------------------------------------------------

# enter this URL to reach PowerShell’s web server
$url = 'http://localhost:8080/'
#$url = 'http://10.0.0.25:8080/'


# connect to dbms using config file


# $global:connectionString = 'Provider=PISQLClient;Data Source=18.217.99.117\anylog; User ID=Jun.Zha;Password=Nan62112!@;Connection Timeout=600'

# $global:connectionString = 'Provider=PIOLEDB.1;Data Source=18.217.99.117'


$global:connection_pool = @{}    # A HASH table that keeps the connections as f(dbms_name)



# start web server
$listener = New-Object System.Net.HttpListener
$listener.Prefixes.Add($url)
$listener.Start()


try
{

   $multi_threads = $True


  while ($listener.IsListening) {  

    # process received request
    $context = $listener.GetContext()
    $Request = $context.Request
 
    $received = '{0} {1}' -f $Request.httpmethod, $Request.url.localpath
    

    # Get the type of request from the header

    $request_type = $Request.Headers["type"]
    $request_details = $Request.Headers["details"]
    $logical_dbms = $Request.Headers["dbms_name"]
    $connect_str = $Request.Headers["connect_str"]

    if (!$request_type -or !$logical_dbms){
        $Response = $context.Response
        $request_type = "Unknown"
        $logical_dbms = "Unknown"
    }else{

        $connection = $global:connection_pool[$logical_dbms]  # Connections are kept as f(database name)

    }

    
    if ($request_type -eq "query"){

        # Get data
        #$query = $request_details

        if ($multi_threads){

            $job = Start-ThreadJob  -ScriptBlock $process_query_job -ArgumentList $connection, $request_details, $context
            Receive-Job -job $job

        }else{
           
            # Single Thread

            Invoke-Command -ScriptBlock $process_query_job -ArgumentList $connection, $request_details, $context

        }
        

    }elseif ($request_type -eq "connect"){

        # Connect or Disconnect
         
         $Response = $context.Response
         
        if  ($request_details -eq "open"){

           # Open connection - need  to have a connect string

            $status = connect_to_dbms $logical_dbms $connect_str

            if ($status -eq $true){
                # connected
                $buffer = "{""status"":""Connected""}"
                $Response.statuscode = 200
            }else{
                # The server was unavailable
                $buffer = "{""status"":""Server not available""}"
                $Response.statuscode = 503
            }
          

            push_data $Response $buffer
            $Response.Close()

        }elseif ($request_details -eq "close"){
            # Close connection, if all connections are closed, exit from driver

            $Response = $context.Response
            $Response.Close()
            
            $global:connection_pool.Remove($logical_dbms)

            if (!$global:connection_pool.Count){
                Write-Host "All connections are closed, terminating driver"
                break
            }

        }else{
                # Unknown
                $Response = $context.Response
                $buffer = "{""status"":""Unknown details with connect request""}"
                $Response.statuscode = 400
                push_data $Response $buffer
                $Response.Close()
        }

    }else{
        # Unknown
        $buffer = "{""status"":""Unknown request""}"
        $Response.statuscode = 400
        push_data $Response $buffer
        $Response.Close()
    }


    Write-Host "Processed AnyLog Request: '$request_type' DBMS: '$logical_dbms'"


  }
}
finally
{
  $listener.Stop()
}
