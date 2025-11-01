/**
 * Password hashing utilities using Web Crypto API
 * Uses PBKDF2 for secure password hashing
 */

/**
 * Convert hex string to ArrayBuffer
 */
function hexToArrayBuffer(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
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
  const computedHash = await hashPassword(password, salt, iterations);
  // Constant-time comparison to prevent timing attacks
  if (computedHash.length !== storedHash.length) {
    return false;
  }
  let result = 0;
  for (let i = 0; i < computedHash.length; i++) {
    result |= computedHash.charCodeAt(i) ^ storedHash.charCodeAt(i);
  }
  return result === 0;
}

// Export for use in script.js
if (typeof window !== 'undefined') {
  window.AuthUtils = {
    hashPassword,
    verifyPassword,
    generateSalt
  };
}

