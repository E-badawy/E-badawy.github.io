(() => {
  const markExternalLinks = () => {
    document.querySelectorAll('a[href]').forEach((link) => {
      const rawHref = link.getAttribute('href');
      if (!rawHref) return;
      if (
        rawHref.startsWith('#') ||
        rawHref.startsWith('/') ||
        rawHref.startsWith('mailto:') ||
        rawHref.startsWith('tel:')
      ) {
        return;
      }

      try {
        const url = new URL(rawHref, window.location.origin);
        const isExternalHttp =
          (url.protocol === 'http:' || url.protocol === 'https:') &&
          url.hostname !== window.location.hostname;

        if (isExternalHttp) {
          link.target = '_blank';
          link.rel = 'noopener noreferrer';
        } else if (link.target === '_blank' && !link.hasAttribute('data-keep-blank')) {
          link.removeAttribute('target');
          if (
            link.rel === 'noopener noreferrer' ||
            link.rel === 'noreferrer noopener' ||
            link.rel === 'noopener' ||
            link.rel === 'noreferrer'
          ) {
            link.removeAttribute('rel');
          }
        }
      } catch {
        // Ignore malformed href values.
      }
    });
  };

  markExternalLinks();
})();
