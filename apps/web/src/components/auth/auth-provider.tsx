"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import { ApiError, login, makeClient, refresh } from "@/lib/api";
import type { ApiClient } from "@/lib/api";
import type { UserInfo } from "@/types/api";

type AuthContextValue = {
  user: UserInfo | null;
  accessToken: string | null;
  refreshToken: string | null;
  loading: boolean;
  isAuthenticated: boolean;
  hasRole: (role: string) => boolean;
  signIn: (email: string, password: string) => Promise<void>;
  signOut: () => void;
  apiCall: <T>(operation: (client: ApiClient) => Promise<T>) => Promise<T>;
};

const ACCESS_TOKEN_KEY = "xenna_access_token";
const REFRESH_TOKEN_KEY = "xenna_refresh_token";

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [accessToken, setAccessToken] = useState<string | null>(null);
  const [refreshToken, setRefreshToken] = useState<string | null>(null);
  const [user, setUser] = useState<UserInfo | null>(null);
  const [loading, setLoading] = useState(true);

  const persistTokens = useCallback((nextAccess: string | null, nextRefresh: string | null) => {
    setAccessToken(nextAccess);
    setRefreshToken(nextRefresh);

    if (nextAccess) {
      localStorage.setItem(ACCESS_TOKEN_KEY, nextAccess);
    } else {
      localStorage.removeItem(ACCESS_TOKEN_KEY);
    }

    if (nextRefresh) {
      localStorage.setItem(REFRESH_TOKEN_KEY, nextRefresh);
    } else {
      localStorage.removeItem(REFRESH_TOKEN_KEY);
    }
  }, []);

  const currentClient = useMemo(() => (accessToken ? makeClient(accessToken) : null), [accessToken]);

  const loadUser = useCallback(
    async (token: string) => {
      const client = makeClient(token);
      const me = await client.getMe();
      setUser(me);
    },
    [],
  );

  const refreshSession = useCallback(async (): Promise<string | null> => {
    if (!refreshToken) {
      return null;
    }

    try {
      const tokens = await refresh(refreshToken);
      persistTokens(tokens.access_token, tokens.refresh_token);
      await loadUser(tokens.access_token);
      return tokens.access_token;
    } catch {
      persistTokens(null, null);
      setUser(null);
      return null;
    }
  }, [loadUser, persistTokens, refreshToken]);

  useEffect(() => {
    let mounted = true;

    const init = async () => {
      const storedAccess = localStorage.getItem(ACCESS_TOKEN_KEY);
      const storedRefresh = localStorage.getItem(REFRESH_TOKEN_KEY);

      if (!mounted) {
        return;
      }

      if (!storedAccess) {
        persistTokens(null, storedRefresh);
        setLoading(false);
        return;
      }

      persistTokens(storedAccess, storedRefresh);
      try {
        await loadUser(storedAccess);
      } catch {
        if (storedRefresh) {
          await refreshSession();
        } else {
          persistTokens(null, null);
          setUser(null);
        }
      } finally {
        if (mounted) {
          setLoading(false);
        }
      }
    };

    init();

    return () => {
      mounted = false;
    };
  }, [loadUser, persistTokens, refreshSession]);

  const signIn = useCallback(
    async (email: string, password: string) => {
      const tokens = await login(email, password);
      persistTokens(tokens.access_token, tokens.refresh_token);
      await loadUser(tokens.access_token);
    },
    [loadUser, persistTokens],
  );

  const signOut = useCallback(() => {
    persistTokens(null, null);
    setUser(null);
  }, [persistTokens]);

  const apiCall = useCallback(
    async <T,>(operation: (client: ApiClient) => Promise<T>): Promise<T> => {
      if (!currentClient || !accessToken) {
        throw new Error("Not authenticated");
      }

      try {
        return await operation(currentClient);
      } catch (error) {
        if (error instanceof ApiError && error.status === 401) {
          const nextToken = await refreshSession();
          if (!nextToken) {
            throw error;
          }
          return operation(makeClient(nextToken));
        }
        throw error;
      }
    },
    [accessToken, currentClient, refreshSession],
  );

  const value = useMemo<AuthContextValue>(
    () => ({
      user,
      accessToken,
      refreshToken,
      loading,
      isAuthenticated: Boolean(accessToken && user),
      hasRole: (role) => Boolean(user?.roles.includes(role as never)),
      signIn,
      signOut,
      apiCall,
    }),
    [accessToken, apiCall, loading, refreshToken, signIn, signOut, user],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within AuthProvider");
  }
  return context;
}
