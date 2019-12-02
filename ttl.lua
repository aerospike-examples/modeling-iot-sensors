function touchttl(rec)
  record.set_ttl(rec, 0)
  aerospike:update(rec)
end
