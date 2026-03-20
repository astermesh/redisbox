/**
 * ACL user model and store.
 *
 * Manages Redis-compatible users with passwords, enabled/disabled state,
 * command permissions, and key patterns. The default user is always present
 * and mirrors the behaviour controlled by the `requirepass` config.
 */

// ---------------------------------------------------------------------------
// AclUser
// ---------------------------------------------------------------------------

export class AclUser {
  readonly username: string;

  /** Plain-text passwords the user can authenticate with. */
  private readonly passwords = new Set<string>();

  /** When true the user can authenticate without a password. */
  nopass = true;

  enabled = true;

  /** When true the user has access to all commands (+@all). */
  allCommands = true;

  /** When true the user has access to all keys (~*). */
  allKeys = true;

  /** When true the user has access to all pub/sub channels (&*). */
  allChannels = true;

  constructor(username: string) {
    this.username = username;
  }

  // --- password management --------------------------------------------------

  addPassword(password: string): void {
    this.passwords.add(password);
    this.nopass = false;
  }

  removePassword(password: string): boolean {
    return this.passwords.delete(password);
  }

  clearPasswords(): void {
    this.passwords.clear();
  }

  /** Set the user to accept any password (and clear stored passwords). */
  setNopass(): void {
    this.passwords.clear();
    this.nopass = true;
  }

  /** Reset the user to accept a single password (clears previous ones). */
  resetPassword(password: string): void {
    this.passwords.clear();
    this.passwords.add(password);
    this.nopass = false;
  }

  hasPasswords(): boolean {
    return this.passwords.size > 0;
  }

  /** Return a snapshot of currently stored passwords (for testing / ACL GETUSER). */
  getPasswords(): string[] {
    return [...this.passwords];
  }

  validatePassword(password: string): boolean {
    if (this.nopass) return true;
    return this.passwords.has(password);
  }

  // --- full reset -----------------------------------------------------------

  /** Reset user to default-user-like state: enabled, nopass, all perms. */
  resetToDefaults(): void {
    this.enabled = true;
    this.nopass = true;
    this.passwords.clear();
    this.allCommands = true;
    this.allKeys = true;
    this.allChannels = true;
  }
}

// ---------------------------------------------------------------------------
// AclStore
// ---------------------------------------------------------------------------

export class AclStore {
  private readonly users = new Map<string, AclUser>();

  constructor() {
    // The default user always exists, starts enabled with nopass + all perms.
    const defaultUser = new AclUser('default');
    this.users.set('default', defaultUser);
  }

  // --- user access ----------------------------------------------------------

  getUser(username: string): AclUser | undefined {
    return this.users.get(username);
  }

  getDefaultUser(): AclUser {
    // The default user is guaranteed to exist — created in constructor.
    const user = this.users.get('default');
    if (!user) {
      throw new Error('Default ACL user missing');
    }
    return user;
  }

  hasUser(username: string): boolean {
    return this.users.has(username);
  }

  /** Return all usernames in insertion order. */
  usernames(): string[] {
    return [...this.users.keys()];
  }

  // --- requirepass synchronisation ------------------------------------------

  /**
   * Synchronise the default user's password with the `requirepass` config
   * value. Call this whenever requirepass may have changed (e.g. before
   * authentication checks).
   *
   * - Empty string → default user gets nopass (any/no password accepted).
   * - Non-empty → default user gets exactly that password, nopass cleared.
   */
  syncRequirePass(password: string): void {
    const user = this.getDefaultUser();
    if (password) {
      user.resetPassword(password);
    } else {
      user.setNopass();
    }
  }

  // --- authentication -------------------------------------------------------

  /**
   * Authenticate a username/password pair.
   *
   * Returns `true` when the user exists, is enabled, and the password is
   * valid (or the user has `nopass`). Returns `false` otherwise.
   */
  authenticate(username: string, password: string): boolean {
    const user = this.users.get(username);
    if (!user || !user.enabled) return false;
    return user.validatePassword(password);
  }
}
