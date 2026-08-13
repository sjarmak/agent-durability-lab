const state = {
  catalog: null,
  scenario: null,
  episodeIndex: 1,
  eventIndex: 0,
  evidenceView: 'normalized',
};

const byId = (id) => document.getElementById(id);

const setText = (id, value) => {
  const element = byId(id);
  if (element) element.textContent = value ?? '';
};

const element = (tag, options = {}) => {
  const node = document.createElement(tag);
  if (options.className) node.className = options.className;
  if (options.text !== undefined) node.textContent = options.text;
  for (const [name, value] of Object.entries(options.attributes ?? {})) {
    node.setAttribute(name, value);
  }
  return node;
};

const artifactURL = (episode, selector) =>
  `/api/episodes/${encodeURIComponent(episode.id)}/artifacts/${encodeURIComponent(selector)}`;

const titleCase = (value) =>
  value
    .split(/[-_]/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');

const shortIdentity = (value) => {
  if (!value || value.length <= 34) return value;
  return `${value.slice(0, 15)}…${value.slice(-12)}`;
};

const selectedEpisode = () => state.scenario.episodes[state.episodeIndex];

const relativeTime = (event, firstEvent) => {
  const current = Date.parse(event.occurred_at);
  const first = Date.parse(firstEvent.occurred_at);
  if (!Number.isFinite(current) || !Number.isFinite(first)) return `seq ${event.sequence}`;
  const milliseconds = Math.max(0, current - first);
  return milliseconds < 1000 ? `+${milliseconds} ms` : `+${(milliseconds / 1000).toFixed(2)} s`;
};

const renderScenario = () => {
  const { product } = state.catalog;
  const scenario = state.scenario;
  setText('product-positioning', product.positioning);
  setText('scenario-title', scenario.title);
  setText('scenario-summary', scenario.summary);
  setText('scenario-question', scenario.question);
  setText('scenario-invariant', scenario.invariant);
  setText('scenario-fault', scenario.failure_boundary);
  setText('falsifier', scenario.falsifier);
  setText('claim-scope', scenario.claim.scope);
  for (const id of ['scenario', 'comparison', 'episode', 'evidence-files', 'limits']) {
    byId(id).hidden = false;
  }
  document.querySelector('.responsibility').hidden = false;
  renderComparison();
  renderEpisodeTabs();
  renderResponsibility();
  selectEpisode(state.episodeIndex, false);
};

const renderComparison = () => {
  const body = byId('comparison-body');
  body.replaceChildren();
  for (const episode of state.scenario.episodes) {
    const row = element('tr', { attributes: { 'data-variant': episode.variant } });
    const variantCell = element('th', { attributes: { scope: 'row' } });
    const variant = element('span', {
      className: `status-mark ${episode.variant}`,
      text: titleCase(episode.variant),
    });
    variantCell.append(variant);
    row.append(
      variantCell,
      element('td', { text: episode.verdict }),
      element('td', { text: String(episode.outcome.physical_effect_count) }),
      element('td', { text: titleCase(episode.authority.at(-1)?.status ?? 'unknown') }),
      element('td', { text: episode.outcome.summary }),
    );
    body.append(row);
  }
};

const renderEpisodeTabs = () => {
  const tabs = byId('episode-tabs');
  tabs.replaceChildren();
  state.scenario.episodes.forEach((episode, index) => {
    const button = element('button', {
      text: titleCase(episode.variant),
      attributes: {
        type: 'button',
        role: 'tab',
        'aria-selected': index === state.episodeIndex ? 'true' : 'false',
        tabindex: index === state.episodeIndex ? '0' : '-1',
      },
    });
    button.addEventListener('click', () => selectEpisode(index, true));
    button.addEventListener('keydown', (event) => {
      if (event.key !== 'ArrowRight' && event.key !== 'ArrowLeft') return;
      event.preventDefault();
      const direction = event.key === 'ArrowRight' ? 1 : -1;
      const next = (index + direction + state.scenario.episodes.length) % state.scenario.episodes.length;
      selectEpisode(next, true);
      tabs.children[next].focus();
    });
    tabs.append(button);
  });
};

const selectEpisode = (index, updateHash) => {
  state.episodeIndex = index;
  state.eventIndex = 0;
  state.evidenceView = 'normalized';
  const episode = selectedEpisode();
  [...byId('episode-tabs').children].forEach((button, position) => {
    button.setAttribute('aria-selected', position === index ? 'true' : 'false');
    button.setAttribute('tabindex', position === index ? '0' : '-1');
  });
  if (updateHash) history.replaceState(null, '', `#${episode.variant}`);
  setText('episode-outcome', episode.outcome.summary);
  setText('episode-verdict', episode.verdict);
  setText('episode-effects', String(episode.outcome.physical_effect_count));
  setText('episode-attempt', String(episode.identities.delivery.activity_attempt));
  setText('episode-replay', episode.provenance.replay_status);
  renderTimeline();
  renderIdentity();
  renderAuthority();
  renderEffects();
  selectEvidenceView('normalized');
};

const renderTimeline = () => {
  const episode = selectedEpisode();
  const list = byId('event-list');
  list.replaceChildren();
  episode.events.forEach((event, index) => {
    const item = element('li', { attributes: { 'data-lane': event.lane } });
    const button = element('button', {
      attributes: {
        type: 'button',
        'aria-current': index === state.eventIndex ? 'step' : 'false',
        'aria-label': `${event.sequence}. ${titleCase(event.event_type)}, ${titleCase(event.lane)}`,
      },
    });
    button.append(
      element('span', { className: 'event-type', text: titleCase(event.event_type) }),
      element('span', { className: 'event-lane', text: event.lane }),
      element('span', { className: 'event-time', text: relativeTime(event, episode.events[0]) }),
    );
    button.addEventListener('click', () => selectEvent(index));
    item.append(button);
    list.append(item);
  });
  selectEvent(0);
};

const selectEvent = (index) => {
  state.eventIndex = index;
  const episode = selectedEpisode();
  const event = episode.events[index];
  [...byId('event-list').querySelectorAll('button')].forEach((button, position) => {
    button.setAttribute('aria-current', position === index ? 'step' : 'false');
  });
  setText('timeline-progress', `${index + 1} of ${episode.events.length}`);
  setText('event-lane', titleCase(event.lane));
  setText('event-title', titleCase(event.event_type));
  setText('event-summary', event.summary);
  const references = byId('event-references');
  references.replaceChildren();
  event.references.forEach((reference, position) => {
    const row = element('div');
    row.append(
      element('dt', { text: `Reference ${position + 1}` }),
      element('dd', { text: reference }),
    );
    references.append(row);
  });
};

const renderIdentity = () => {
  const { identities } = selectedEpisode();
  const rows = [
    ['Logical', 'Session', identities.session_id],
    ['Logical', 'Turn', identities.turn_id],
    ['Logical', 'Operation', identities.operation_id],
    ['Logical', 'Effect', identities.effect_id],
    ['Temporal delivery', 'Workflow', identities.delivery.workflow_id],
    ['Temporal delivery', 'Run', identities.delivery.run_id],
    ['Temporal delivery', 'Activity', identities.delivery.activity_id],
    ['Observation', 'Worker', identities.delivery.worker_identity],
    ['Observation', 'Process', identities.delivery.process_identity],
    ['Observation', 'Provider', identities.delivery.provider_identity || 'not recorded'],
  ];
  const body = byId('identity-body');
  body.replaceChildren();
  for (const [kind, label, value] of rows) {
    const row = element('tr');
    row.append(
      element('th', { text: kind, attributes: { scope: 'row' } }),
      element('td', { text: label }),
      element('td', { text: shortIdentity(value), attributes: { title: value } }),
    );
    body.append(row);
  }
};

const renderAuthority = () => {
  const list = byId('authority-list');
  list.replaceChildren();
  selectedEpisode().authority.forEach((authority) => {
    const item = element('li');
    item.append(
      element('strong', { text: `Generation ${authority.generation}: ${titleCase(authority.status)}` }),
      element('p', { text: `Actor ${authority.actor}` }),
      element('p', { className: 'record-meta', text: authority.capability_digest || 'No capability authority recorded' }),
    );
    list.append(item);
  });
};

const renderEffects = () => {
  const list = byId('effect-list');
  list.replaceChildren();
  selectedEpisode().effects.forEach((effect, index) => {
    const item = element('li');
    item.append(
      element('strong', { text: `Physical receipt ${index + 1}: ${titleCase(effect.disposition)}` }),
      element('p', { text: effect.summary }),
      element('p', { className: 'record-meta', text: effect.receipt_id }),
    );
    list.append(item);
  });
};

const renderResponsibility = () => {
  const list = byId('responsibility-list');
  list.replaceChildren();
  const responsibilities = state.scenario.responsibility;
  for (const key of ['temporal', 'application', 'destination', 'executor']) {
    const item = element('div');
    item.append(element('dt', { text: titleCase(key) }), element('dd', { text: responsibilities[key] }));
    list.append(item);
  }
};

const selectEvidenceView = (view) => {
  state.evidenceView = view;
  document.querySelectorAll('[data-evidence-view]').forEach((button) => {
    button.setAttribute('aria-selected', button.dataset.evidenceView === view ? 'true' : 'false');
  });
  const panel = byId('evidence-panel');
  panel.replaceChildren();
  if (view === 'normalized') renderNormalizedEvidence(panel);
  if (view === 'native') renderArtifactEvidence(panel, 'history', selectedEpisode().native_history, 'Native Temporal history');
  if (view === 'raw') renderArtifactEvidence(panel, 'raw-0', selectedEpisode().raw_evidence[0], 'Raw trial record');
  if (view === 'provenance') renderProvenance(panel);
};

const renderNormalizedEvidence = (panel) => {
  panel.append(
    element('h3', { text: 'Explanatory event view' }),
    element('p', { text: 'This sequence is normalized for navigation. It is not the oracle and does not replace the native Temporal Event History or raw trial record.' }),
  );
  const pre = element('pre', { attributes: { tabindex: '0', 'aria-label': 'Normalized event JSON' } });
  pre.append(element('code', { text: JSON.stringify(selectedEpisode().events, null, 2) }));
  panel.append(pre);
};

const renderArtifactEvidence = (panel, selector, link, title) => {
  panel.append(
    element('h3', { text: title }),
    element('p', { text: `${link.label}. The backend re-verifies the sealed transport and exact archive member before returning bytes.` }),
  );
  const actions = element('div', { className: 'artifact-actions' });
  const load = element('button', { text: 'Load verified record', attributes: { type: 'button' } });
  const open = element('a', {
    text: 'Open raw response',
    attributes: { href: artifactURL(selectedEpisode(), selector), target: '_blank', rel: 'noreferrer' },
  });
  const output = element('pre', {
    attributes: { 'aria-live': 'polite', tabindex: '0', 'aria-label': `${title} JSON` },
  });
  load.addEventListener('click', async () => {
    load.disabled = true;
    load.textContent = 'Verifying…';
    output.textContent = 'Re-verifying transport, manifest, archive, and member…';
    try {
      const response = await fetch(artifactURL(selectedEpisode(), selector), { cache: 'no-store' });
      if (!response.ok) throw new Error(`Evidence request failed with ${response.status}`);
      const data = await response.json();
      output.textContent = JSON.stringify(data, null, 2);
      load.textContent = 'Verified record loaded';
    } catch (error) {
      output.textContent = error instanceof Error ? error.message : 'Evidence request failed';
      load.textContent = 'Retry verification';
    } finally {
      load.disabled = false;
    }
  });
  actions.append(load, open);
  panel.append(actions, output);
};

const renderProvenance = (panel) => {
  const episode = selectedEpisode();
  panel.append(
    element('h3', { text: 'Source and correction lineage' }),
    element('p', { text: `Source revision: ${episode.provenance.source_revision}. Replay status: ${episode.provenance.replay_status}.` }),
  );
  const actions = element('div', { className: 'artifact-actions' });
  const links = [
    ['Manifest', 'manifest'],
    ['Independent audit', 'report'],
    ...episode.provenance.correction_lineage.map((_, index) => [`Correction lineage ${index + 1}`, `lineage-${index}`]),
  ];
  for (const [label, selector] of links) {
    actions.append(element('a', {
      text: label,
      attributes: { href: artifactURL(episode, selector), target: '_blank', rel: 'noreferrer' },
    }));
  }
  panel.append(actions);
};

document.querySelectorAll('[data-evidence-view]').forEach((button) => {
  button.addEventListener('click', () => selectEvidenceView(button.dataset.evidenceView));
});

const loadCatalog = async () => {
  try {
    const response = await fetch('/api/catalog', { cache: 'no-store' });
    if (!response.ok) throw new Error(`Catalog request failed with ${response.status}`);
    state.catalog = await response.json();
    state.scenario = state.catalog.scenarios[0];
    const requestedVariant = window.location.hash.slice(1);
    const requestedIndex = state.scenario.episodes.findIndex((episode) => episode.variant === requestedVariant);
    if (requestedIndex >= 0) state.episodeIndex = requestedIndex;
    renderScenario();
    setText('loading-status', 'Verified presentation catalog loaded. Select an episode or inspect the record.');
  } catch (error) {
    setText('loading-status', error instanceof Error ? error.message : 'Verified evidence is unavailable.');
  }
};

loadCatalog();
