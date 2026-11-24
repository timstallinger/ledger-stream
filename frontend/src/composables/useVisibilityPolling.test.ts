import { mount } from '@vue/test-utils';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { defineComponent } from 'vue';
import { useVisibilityPolling } from './useVisibilityPolling';

describe('useVisibilityPolling', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      get: () => 'visible'
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('polls on interval, pauses when hidden, backs off on error, and clears interval on unmount', async () => {
    let calls = 0;
    let shouldFail = false;

    const TestHost = defineComponent({
      setup() {
        const polling = useVisibilityPolling(
          async () => {
            calls += 1;
            if (shouldFail) {
              throw new Error('backend down');
            }
          },
          { baseIntervalMs: 1000, maxIntervalMs: 8000, enabled: () => true }
        );
        return { polling };
      },
      template: '<div />'
    });

    const wrapper = mount(TestHost);
    await vi.advanceTimersByTimeAsync(1000);
    expect(calls).toBe(1);

    // Trigger error -> backs off to 2000ms
    shouldFail = true;
    await vi.advanceTimersByTimeAsync(1000);
    expect(calls).toBe(2);
    expect(wrapper.vm.polling.currentIntervalMs.value).toBe(2000);

    // Hide tab -> pauses polling completely
    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      get: () => 'hidden'
    });
    document.dispatchEvent(new Event('visibilitychange'));
    expect(wrapper.vm.polling.isPausedHidden.value).toBe(true);

    await vi.advanceTimersByTimeAsync(10000);
    expect(calls).toBe(2);

    // Unmount clears interval cleanly
    wrapper.unmount();
    await vi.advanceTimersByTimeAsync(10000);
    expect(calls).toBe(2);
  });
});
