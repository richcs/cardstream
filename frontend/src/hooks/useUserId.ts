const STORAGE_KEY = 'cardstream:userId';

/** Anonymous client-generated id (localStorage) that scopes watchlists — no auth in MVP. */
export function getUserId(): string {
  let id = localStorage.getItem(STORAGE_KEY);
  if (!id) {
    id = crypto.randomUUID();
    localStorage.setItem(STORAGE_KEY, id);
  }
  return id;
}
