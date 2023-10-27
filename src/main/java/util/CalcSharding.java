package util;

import itec.ldap.LDAPConnection;
import itec.ldap.LDAPException;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class CalcSharding {

   public static Optional<Integer> calcShardingNum(String sharding, int connSizePerDC) {
       Optional<?> optional = Optional.empty();
       try {
          LDAPConnection conn = new LDAPConnection();
          int policy = 0;
          conn.setShardingPolicy(policy);
          int shardingOffset = 1;
          int shardingNum = conn.calcShardingNum(sharding, connSizePerDC) - shardingOffset;
          optional = Optional.of(Integer.valueOf(shardingNum));
       } catch (LDAPException e) {
          log.error("LDAPConnection calcShardingNum Exception ldapExceptionContent:{}", e);
       } catch (Exception e) {
          log.error("LDAPConnection calcShardingNum Exception", e);
       }
       return (Optional)optional;
   }
}