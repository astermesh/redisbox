import type { AclUser } from '../../acl-store.ts';

// ---------------------------------------------------------------------------
// ACL SETUSER — apply rules to a user
// ---------------------------------------------------------------------------

export function applyRule(user: AclUser, rule: string): string | null {
  switch (rule) {
    case 'on':
      user.enabled = true;
      return null;
    case 'off':
      user.enabled = false;
      return null;
    case 'nopass':
      user.setNopass();
      return null;
    case 'resetpass':
      user.clearPasswords();
      user.nopass = false;
      return null;
    case 'reset':
      user.resetToDefaults();
      // reset in SETUSER context: off, no passwords, no perms
      user.enabled = false;
      user.nopass = false;
      user.allCommands = false;
      user.allKeys = false;
      user.allChannels = false;
      return null;
    case 'allcommands':
      user.allCommands = true;
      return null;
    case 'nocommands':
      user.allCommands = false;
      return null;
    case 'allkeys':
      user.allKeys = true;
      return null;
    case 'resetkeys':
      user.allKeys = false;
      return null;
    case 'allchannels':
      user.allChannels = true;
      return null;
    case 'resetchannels':
      user.allChannels = false;
      return null;
    default:
      break;
  }

  // >password — add password
  if (rule.startsWith('>')) {
    user.addPassword(rule.slice(1));
    return null;
  }

  // <password — remove password
  if (rule.startsWith('<')) {
    const pw = rule.slice(1);
    if (!user.removePassword(pw)) {
      return `ERR Error in ACL SETUSER modifier '<...>': no such password`;
    }
    return null;
  }

  // #hash — add password by hash (we store hash as-is for display, but
  // cannot match against AUTH plaintext). Accept for compatibility.
  if (rule.startsWith('#')) {
    // For emulator simplicity, accept but warn this won't work with AUTH
    return null;
  }

  // !hash — remove password by hash
  if (rule.startsWith('!')) {
    return null;
  }

  // ~pattern — key pattern
  if (rule.startsWith('~')) {
    if (rule === '~*') {
      user.allKeys = true;
    }
    return null;
  }

  // %R~, %W~, %RW~ — key pattern with read/write perms
  if (rule.startsWith('%')) {
    return null;
  }

  // &pattern — channel pattern
  if (rule.startsWith('&')) {
    if (rule === '&*') {
      user.allChannels = true;
    }
    return null;
  }

  // +@category — allow category
  if (rule.startsWith('+@')) {
    const cat = rule.slice(2);
    if (cat === 'all') {
      user.allCommands = true;
    }
    return null;
  }

  // -@category — deny category
  if (rule.startsWith('-@')) {
    const cat = rule.slice(2);
    if (cat === 'all') {
      user.allCommands = false;
    }
    return null;
  }

  // +command — allow command
  if (rule.startsWith('+')) {
    return null;
  }

  // -command — deny command
  if (rule.startsWith('-')) {
    return null;
  }

  return `ERR Error in ACL SETUSER modifier '${rule}': Syntax error`;
}
