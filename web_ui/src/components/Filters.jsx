import React from 'react';
import { useI18n } from '../i18n';

const DEFAULTS = {
  court: '', canton: '', language: '', date_from: '', date_to: '',
  collapse_duplicates: true, multilingual: true,
};

export default function Filters({ filters, onChange }) {
  const { t } = useI18n();

  const update = (key, value) => {
    onChange({ ...filters, [key]: value });
  };

  const isDefault = Object.entries(DEFAULTS).every(
    ([k, v]) => filters[k] === v
  );

  return (
    <div className="filters-panel">
      <div className="filter-row">
        <label>
          {t('filter.court')}
          <input
            type="text"
            value={filters.court}
            onChange={e => update('court', e.target.value)}
            placeholder="e.g. bger, bvger"
          />
        </label>
        <label>
          {t('filter.canton')}
          <input
            type="text"
            value={filters.canton}
            onChange={e => update('canton', e.target.value)}
            placeholder="e.g. ZH, BE, GE"
          />
        </label>
        <label>
          {t('filter.language')}
          <select value={filters.language} onChange={e => update('language', e.target.value)}>
            <option value="">{t('filter.all')}</option>
            <option value="de">Deutsch</option>
            <option value="fr">Fran&ccedil;ais</option>
            <option value="it">Italiano</option>
            <option value="rm">Rumantsch</option>
          </select>
        </label>
      </div>
      <div className="filter-row">
        <label>
          {t('filter.from')}
          <input
            type="date"
            value={filters.date_from}
            onChange={e => update('date_from', e.target.value)}
          />
        </label>
        <label>
          {t('filter.to')}
          <input
            type="date"
            value={filters.date_to}
            onChange={e => update('date_to', e.target.value)}
          />
        </label>
        <label className="toggle-label">
          <input
            type="checkbox"
            checked={filters.collapse_duplicates}
            onChange={e => update('collapse_duplicates', e.target.checked)}
          />
          {t('filter.collapse')}
        </label>
        <label className="toggle-label">
          <input
            type="checkbox"
            checked={filters.multilingual}
            onChange={e => update('multilingual', e.target.checked)}
          />
          {t('filter.multilingual')}
        </label>
        {!isDefault && (
          <button
            type="button"
            className="btn-secondary"
            onClick={() => onChange({ ...DEFAULTS })}
          >
            {t('filter.reset')}
          </button>
        )}
      </div>
    </div>
  );
}
