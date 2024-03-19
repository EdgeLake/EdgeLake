/*
By using this source code, you acknowledge that this software in source code form remains a confidential information of AnyLog, Inc.,
and you shall not transfer it to any other party without AnyLog, Inc.'s prior written consent. You further acknowledge that all right,
title and interest in and to this source code, and any copies and/or derivatives thereof and all documentation, which describes
and/or composes such source code or any such derivatives, shall remain the sole and exclusive property of AnyLog, Inc.,
and you shall not edit, reverse engineer, copy, emulate, create derivatives of, compile or decompile or otherwise tamper or modify
this source code in any way, or allow others to do so. In the event of any such editing, reverse engineering, copying, emulation,
creation of derivative, compilation, decompilation, tampering or modification of this source code by you, or any of your affiliates (term
to be broadly interpreted) you or your such affiliates shall unconditionally assign and transfer any intellectual property created by any
such non-permitted act to AnyLog, Inc.
*/

#include <eosio/eosio.hpp>

using namespace eosio;

class [[eosio::contract("aleos")]] aleos : public contract {	// Explicitly named the contract to "aleos"
  public:

     using contract::contract;

      // Constractor
      aleos(name receiver, name code, datastream<const char*> ds):contract(receiver, code, ds){}


      [[eosio::action]]
      void add( name user, uint64_t json_id, std::string json_data) {
         print( "User: ", user, "adding: ", json_data);
    	
	 metadata_index addresses( get_self(), get_first_receiver().value );
	 auto iterator = addresses.find(user.value);
    	 if( iterator == addresses.end() ){
      		addresses.emplace(user, [&]( auto& row ) {
       			row.key = json_id;			// according to table defined below
       			row.metainfo = json_data;	// according to table defined below
      		});
    	 }
    	 else {
		print("  <-- Duplicate key");
    	}
      }

  private:
      struct [[eosio::table]] metadata{
	uint64_t key;
	std::string metainfo;

	uint64_t primary_key() const { return key; }
      };

      typedef eosio::multi_index<"almetadata"_n, metadata> metadata_index;

      // Return the hash value of a string
      checksum256 get_hash(std::string str_to_hash){

      	checksum256 calc_hash;

	//sha256((const char*) str_to_hash.c_str(), str_to_hash.size() * sizeof(char), &calc_hash);
	return calc_hash;
      }
      
};

/*
class [[eosio::contract("addressbook")]] addressbook : public eosio::contract {

public:

  addressbook(name receiver, name code,  datastream<const char*> ds): contract(receiver, code, ds) {}

  [[eosio::action]]
  void upsert(name user, std::string json_data) {
    require_auth( user );
    address_index addresses( get_self(), get_first_receiver().value );
    auto iterator = addresses.find(user.value);
    if( iterator == addresses.end() )
    {
      addresses.emplace(user, [&]( auto& row ) {
       row.key = user;
       row.json_data = json_data;
      });
    }
    else {
      addresses.modify(iterator, user, [&]( auto& row ) {
        row.key = user;
        row.json_data = json_data;
      });
    }
  }

  [[eosio::action]]
  void erase(name user) {
    require_auth(user);

    address_index addresses( get_self(), get_first_receiver().value);

    auto iterator = addresses.find(user.value);
    check(iterator != addresses.end(), "Record does not exist");
    addresses.erase(iterator);
  }

private:
  struct [[eosio::table]] person {
    name key;
    std::string json_data;
    uint64_t primary_key() const { return key.value; }
  };
  typedef eosio::multi_index<"people"_n, person> address_index;

};    
*/

