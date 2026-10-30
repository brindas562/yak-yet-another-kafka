# Common configuration shared by all systems

# Redis (System 3)
REDIS_HOST = "10.144.199.130"   # IP of Redis system
REDIS_PORT = 6379

# Broker nodes (update if IPs differ)
BROKER_1 = "10.144.186.122"     # Leader
BROKER_2 = "10.144.239.240"     # Follower

# Lease settings
LEASE_KEY = "yak:leader"
LEASE_TTL = 5  # seconds