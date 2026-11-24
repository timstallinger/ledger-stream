import { onMounted, onUnmounted, ref, type Ref } from 'vue';

export interface PollingOptions {
  readonly baseIntervalMs?: number;
  readonly maxIntervalMs?: number;
  readonly enabled: () => boolean;
}

export interface PollingController {
  readonly isPausedHidden: Ref<boolean>;
  readonly currentIntervalMs: Ref<number>;
  readonly lastSyncedAt: Ref<string | null>;
  readonly consecutiveErrors: Ref<number>;
  triggerNow(): Promise<void>;
  stop(): void;
}

/**
 * Visibility-aware polling with setInterval + clearInterval in onUnmounted,
 * exponential backoff on failure, and automatic pause when tab is hidden.
 */
export function useVisibilityPolling(
  task: () => Promise<void>,
  options: PollingOptions
): PollingController {
  const baseIntervalMs = options.baseIntervalMs ?? 5000;
  const maxIntervalMs = options.maxIntervalMs ?? 60000;

  const isPausedHidden = ref<boolean>(document.visibilityState === 'hidden');
  const currentIntervalMs = ref<number>(baseIntervalMs);
  const lastSyncedAt = ref<string | null>(null);
  const consecutiveErrors = ref<number>(0);

  let intervalHandle: number | null = null;
  let inFlight = false;

  const clearTimer = (): void => {
    if (intervalHandle !== null) {
      window.clearInterval(intervalHandle);
      intervalHandle = null;
    }
  };

  const scheduleTimer = (intervalMs: number): void => {
    clearTimer();
    if (isPausedHidden.value) {
      return;
    }
    currentIntervalMs.value = intervalMs;
    intervalHandle = window.setInterval(() => {
      void executeTick();
    }, intervalMs);
  };

  const executeTick = async (): Promise<void> => {
    if (inFlight || isPausedHidden.value || !options.enabled()) {
      return;
    }
    inFlight = true;
    try {
      await task();
      lastSyncedAt.value = new Date().toISOString();
      if (consecutiveErrors.value > 0) {
        consecutiveErrors.value = 0;
        scheduleTimer(baseIntervalMs);
      }
    } catch {
      consecutiveErrors.value += 1;
      const nextInterval = Math.min(maxIntervalMs, baseIntervalMs * 2 ** consecutiveErrors.value);
      scheduleTimer(nextInterval);
    } finally {
      inFlight = false;
    }
  };

  const onVisibilityChange = (): void => {
    const hidden = document.visibilityState === 'hidden';
    isPausedHidden.value = hidden;
    if (hidden) {
      clearTimer();
    } else {
      scheduleTimer(currentIntervalMs.value);
      void executeTick();
    }
  };

  onMounted(() => {
    document.addEventListener('visibilitychange', onVisibilityChange);
    if (!isPausedHidden.value) {
      scheduleTimer(baseIntervalMs);
    }
  });

  onUnmounted(() => {
    clearTimer();
    document.removeEventListener('visibilitychange', onVisibilityChange);
  });

  return {
    isPausedHidden,
    currentIntervalMs,
    lastSyncedAt,
    consecutiveErrors,
    triggerNow: executeTick,
    stop: clearTimer
  };
}
