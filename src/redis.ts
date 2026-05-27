import { RedisClient } from '@devvit/public-api';

export interface PostRecord {
    id: string;
    timestamp: number;
}

export const getUserPostTimestamps = async (redis: RedisClient, subredditId: string, authorName: string): Promise<PostRecord[]> => {
    const key = `history:${subredditId}:${authorName}`;
    const data = await redis.get(key);
    if (!data) return [];
    try {
        const parsed = JSON.parse(data);
        // Handle migration from old number[] schema
        if (parsed.length > 0 && typeof parsed[0] === 'number') {
             return parsed.map((ts: number) => ({ id: 'unknown', timestamp: ts }));
        }
        return parsed;
    } catch (e) {
        return [];
    }
};

export const saveUserPostTimestamps = async (redis: RedisClient, subredditId: string, authorName: string, records: PostRecord[], ttlMs: number) => {
    const key = `history:${subredditId}:${authorName}`;
    await redis.set(key, JSON.stringify(records));
    await redis.expire(key, Math.ceil(ttlMs / 1000));
};

export const getViolations = async (redis: RedisClient, subredditId: string, authorName: string): Promise<number[]> => {
    const key = `violations:${subredditId}:${authorName}`;
    const data = await redis.get(key);
    if (!data) return [];
    try {
        const parsed = JSON.parse(data);
        if (Array.isArray(parsed) && (parsed.length === 0 || typeof parsed[0] === 'number')) {
             return parsed;
        }
        return []; // Reset legacy string counts to empty array
    } catch (e) {
        return []; // Reset legacy string counts to empty array
    }
};

export const recordAndCountViolations = async (redis: RedisClient, subredditId: string, authorName: string, windowDays: number): Promise<number> => {
    const key = `violations:${subredditId}:${authorName}`;
    const now = Date.now();
    const cutoffTime = now - (windowDays * 24 * 60 * 60 * 1000);
    
    let violations = await getViolations(redis, subredditId, authorName);
    
    // Prune old violations
    violations = violations.filter(ts => ts > cutoffTime);
    
    // Add current violation
    violations.push(now);
    
    await redis.set(key, JSON.stringify(violations));
    await redis.expire(key, Math.ceil(windowDays * 24 * 60 * 60)); // TTL for the array
    
    return violations.length;
};

export const hasHallpass = async (redis: RedisClient, subredditId: string, authorName: string): Promise<boolean> => {
    const key = `hallpass:${subredditId}:${authorName}`;
    const data = await redis.get(key);
    return data === 'true';
};

export const consumeHallpass = async (redis: RedisClient, subredditId: string, authorName: string) => {
    const key = `hallpass:${subredditId}:${authorName}`;
    await redis.del(key);
};

export const grantHallpass = async (redis: RedisClient, subredditId: string, authorName: string) => {
    const key = `hallpass:${subredditId}:${authorName}`;
    await redis.set(key, 'true');
    await redis.expire(key, 30 * 24 * 60 * 60); // 30 day expiration on hallpasses
};
