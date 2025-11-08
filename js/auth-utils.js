/**
 * Password hashing utilities using Web Crypto API
 * Uses PBKDF2 for secure password hashing
 */

/**
 * Convert hex string to ArrayBuffer
 */
function hexToArrayBuffer(hex) {
  // Remove any whitespace
  hex = hex.trim();
  // Ensure even length
  if (hex.length % 2 !== 0) {
    throw new Error('Hex string must have even length');
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    const byteValue = parseInt(hex.substring(i, i + 2), 16);
    if (isNaN(byteValue)) {
      throw new Error(`Invalid hex character at position ${i}`);
    }
    bytes[i / 2] = byteValue;
  }
  return bytes.buffer;
}

/**
 * Convert ArrayBuffer to hex string
 */
async function arrayBufferToHex(buffer) {
  const bytes = new Uint8Array(buffer);
  let hex = '';
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i].toString(16).padStart(2, '0');
  }
  return hex;
}

/**
 * Generate a random salt (32 bytes)
 */
async function generateSalt() {
  const array = new Uint8Array(32);
  crypto.getRandomValues(array);
  return arrayBufferToHex(array.buffer);
}

/**
 * Hash a password using PBKDF2
 * @param {string} password - Plain text password
 * @param {string} salt - Hex encoded salt
 * @param {number} iterations - Number of iterations (default: 100000)
 * @returns {Promise<string>} Hex encoded hash
 */
async function hashPassword(password, salt, iterations = 100000) {
  const encoder = new TextEncoder();
  const passwordData = encoder.encode(password);
  const saltData = hexToArrayBuffer(salt);

  const keyMaterial = await crypto.subtle.importKey(
    'raw',
    passwordData,
    'PBKDF2',
    false,
    ['deriveBits']
  );

  const hashBuffer = await crypto.subtle.deriveBits(
    {
      name: 'PBKDF2',
      salt: saltData,
      iterations: iterations,
      hash: 'SHA-256'
    },
    keyMaterial,
    512 // 512 bits = 64 bytes
  );

  return arrayBufferToHex(hashBuffer);
}

/**
 * Verify a password against a stored hash
 * @param {string} password - Plain text password to verify
 * @param {string} storedHash - Hex encoded stored hash
 * @param {string} salt - Hex encoded salt
 * @param {number} iterations - Number of iterations
 * @returns {Promise<boolean>} True if password matches
 */
async function verifyPassword(password, storedHash, salt, iterations = 100000) {
  try {
    // Trim whitespace from inputs
    password = String(password).trim();
    storedHash = String(storedHash).trim();
    salt = String(salt).trim();
    
    // Debug logging (remove in production if desired)
    if (window.DEBUG_PASSWORD_VERIFICATION) {
      console.log('[Password Verify] Input password:', password);
      console.log('[Password Verify] Stored hash length:', storedHash.length);
      console.log('[Password Verify] Salt length:', salt.length);
      console.log('[Password Verify] Iterations:', iterations);
    }
    
    const computedHash = await hashPassword(password, salt, iterations);
    
    if (window.DEBUG_PASSWORD_VERIFICATION) {
      console.log('[Password Verify] Computed hash:', computedHash);
      console.log('[Password Verify] Stored hash:', storedHash);
      console.log('[Password Verify] Hashes match:', computedHash === storedHash);
    }
    
    // Constant-time comparison to prevent timing attacks
    if (computedHash.length !== storedHash.length) {
      if (window.DEBUG_PASSWORD_VERIFICATION) {
        console.error('[Password Verify] Hash length mismatch:', computedHash.length, 'vs', storedHash.length);
      }
      return false;
    }
    let result = 0;
    for (let i = 0; i < computedHash.length; i++) {
      result |= computedHash.charCodeAt(i) ^ storedHash.charCodeAt(i);
    }
    const matches = result === 0;
    
    if (window.DEBUG_PASSWORD_VERIFICATION) {
      console.log('[Password Verify] Result:', matches ? 'MATCH' : 'NO MATCH');
    }
    
    return matches;
  } catch (err) {
    console.error('[Password Verify] Error during verification:', err);
    return false;
  }
}

// Export for use in script.js
if (typeof window !== 'undefined') {
  window.AuthUtils = {
    hashPassword,
    verifyPassword,
    generateSalt
  };
}

