export const abortableDelay = (
  milliseconds: number,
  signal?: AbortSignal,
): Promise<boolean> =>
  new Promise((resolve) => {
    if (signal?.aborted) {
      resolve(false);
      return;
    }

    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve(true);
    }, milliseconds);
    const onAbort = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      resolve(false);
    };

    signal?.addEventListener("abort", onAbort, { once: true });
  });
