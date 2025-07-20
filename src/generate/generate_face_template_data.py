import hashlib
import random
import struct

# =====================
# "face_encoding" data
# =====================
def generate_face_encoding(customer_id):
    """
    Generate encrypted face encoding for biometric authentication
    
    Args:
        customer_id (str): Customer UUID for uniqueness and reproducibility
        
    Returns:
        bytes: Encrypted face encoding as binary data (BYTEA format for PostgreSQL)
    """
    
    # 1. Generate realistic face encoding vector (128 dimensions like FaceNet)
    # Use customer_id as seed for consistency - same customer always gets same encoding
    seed_value = int(hashlib.md5(str(customer_id).encode()).hexdigest()[:8], 16)
    random.seed(seed_value)
    
    # Generate 128-dimensional face encoding vector
    # Values typically range from -1.0 to 1.0 for normalized facial features
    face_vector = []
    for i in range(128):
        # Realistic face encoding distribution
        # Most values cluster around 0, with some outliers
        value = random.gauss(0.0, 0.3)  # Normal distribution with std=0.3
        # Clamp to valid range
        value = max(-1.0, min(1.0, value))
        face_vector.append(value)
    
    # 2. Serialize vector to bytes
    # Pack as 128 float32 values (4 bytes each = 512 bytes total)
    vector_bytes = struct.pack('128f', *face_vector)
    
    # 3. Simple encryption using XOR with key derived from customer_id
    # In production, would use AES-256-GCM, but XOR is sufficient for demo data
    encryption_key = hashlib.sha256(str(customer_id).encode()).digest()[:32]
    
    encrypted_bytes = bytearray()
    for i, byte in enumerate(vector_bytes):
        encrypted_bytes.append(byte ^ encryption_key[i % len(encryption_key)])
    
    return bytes(encrypted_bytes)
