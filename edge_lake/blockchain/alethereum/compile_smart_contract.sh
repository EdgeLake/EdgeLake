# Requires dependency
# npm install -g solc@0.6.1
# https://www.npmjs.com/package/solc


SMART_CONTRACT_FILE=AnyLog2.sol


echo $SMART_CONTRACT_FILE
echo $TMP_FILES
solcjs --bin --abi $SMART_CONTRACT_FILE --output-dir .
