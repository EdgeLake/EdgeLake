public class hl_calls {

    private boolean connected = false;

    /*
    * Flag connection established. This is called after the Java object in the Python code was called without an error
    */
    public void set_connected(){

        connected = true;
    }

    public boolean is_connected(){

        return connected;
    }

     /*
    *  Add a new policy
    *  @param policy_id - the hash value of the policy
    *  @param  policy - the policy info
    *  @return - true(success)/false(error)
    */
    public boolean put_policies(String policy_id, String policy) {
        return false;
    }

     /*
    *  Get all policies
    *  @return - all policies as a JSON string, or empty string if failed or no policies
    */
    public String get_policies() {
        return "123abc";
    }
    
}
