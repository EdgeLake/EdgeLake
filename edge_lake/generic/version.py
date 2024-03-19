'''
By using this source code, you acknowledge that this software in source code form remains a confidential information of AnyLog, Inc.,
and you shall not transfer it to any other party without AnyLog, Inc.'s prior written consent. You further acknowledge that all right,
title and interest in and to this source code, and any copies and/or derivatives thereof and all documentation, which describes
and/or composes such source code or any such derivatives, shall remain the sole and exclusive property of AnyLog, Inc.,
and you shall not edit, reverse engineer, copy, emulate, create derivatives of, compile or decompile or otherwise tamper or modify
this source code in any way, or allow others to do so. In the event of any such editing, reverse engineering, copying, emulation,
creation of derivative, compilation, decompilation, tampering or modification of this source code by you, or any of your affiliates (term
to be broadly interpreted) you or your such affiliates shall unconditionally assign and transfer any intellectual property created by any
such non-permitted act to AnyLog, Inc.
'''
# --------------------------------------------------------------------------------------------------------
# The methods in this file are implemented with a different version - Contact AnyLog for details
# --------------------------------------------------------------------------------------------------------
import anylog_node.generic.process_status as process_status

PUBLIC_KEY_CHARS_ = 0
PUBLIC_KEY_LENGTH_ = 0

def al_auth_get_transfer_str(status, source_data):
    return [process_status.SUCCESS, ""]

def al_auth_is_node_encryption():
    return False

def al_auth_setup_encryption(status, public_str):
    return ["", None]

def al_auth_symetric_encryption(status, f_object, rows_data):
    return None

def al_auth_is_node_authentication():
    return False

def al_auth_is_user_authentication():
    return False

def al_auth_get_signatory_public_key(status):
    return [process_status.Missing_public_key, None]

def al_auth_get_authentication():
    return [process_status.Missing_public_key, None]

def al_auth_verify(status, license_public_str_, signature, conditions):
    return False

def al_auth_decrypt_node_message(status, encryption_key):
    return ""

def al_auth_generate_fernet(status, password, salt):
    return None

def al_auth_symetric_decryption(status, f_object, data):
    return None

def al_auth_validate_basic_auth(status, user_name, password):
    return False

def al_auth_validate_user(status, user_key, test_expiration):
    return False

def aldistributor_is_distr_running():
    return False

def alconsumer_is_consumer_running():
    return False

def alpublisher_is_active():
    return False

def set_public_key_chars( value ):
    global PUBLIC_KEY_CHARS_
    PUBLIC_KEY_CHARS_ = value

def set_public_key_length( value ):
    global PUBLIC_KEY_LENGTH_
    PUBLIC_KEY_LENGTH_ = value

def get_public_key_chars(Value):
    return PUBLIC_KEY_CHARS_

def get_public_key_length(value):
    return PUBLIC_KEY_LENGTH_

def permissions_authenticate_tcp_message(status, mem_view):
    return [process_status.SUCCESS, ""]

def permissions_permission_process(status, depth, source, public_key, command, dbms_name, table_name):
    return True

def permissions_permissions_by_public_key(status, public_key):
    return [process_status.SUCCESS, None]

def permissions_authenticate_rest_message(status, public_key):
    return process_status.SUCCESS

