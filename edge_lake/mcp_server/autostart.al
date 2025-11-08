#----------------------------------------------------------------------------------------------------------------------#
# MCP Server Auto-Start
# This script checks if MCP_AUTOSTART environment variable is set to true and starts the MCP server if configured.
# Called from deployment-scripts/node-deployment/main.al
#----------------------------------------------------------------------------------------------------------------------#
# process !anylog_path/EdgeLake/edge_lake/mcp_server/autostart.al

on error ignore

:check-mcp-autostart:
if !debug_mode == true then print "Check if MCP server should auto-start"

set mcp_autostart = false
if $MCP_AUTOSTART == true or $MCP_AUTOSTART == True or $MCP_AUTOSTART == TRUE then set mcp_autostart = true

if !mcp_autostart == false then goto end-script

:start-mcp-server:
if !debug_mode == true then print "Auto-starting MCP server"

on error goto mcp-start-error
run mcp server

goto end-script

:mcp-start-error:
echo "Warning: Failed to auto-start MCP server (REST server may not be running yet)"
goto end-script

:end-script:
end script
