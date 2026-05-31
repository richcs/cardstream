-- Phase 1 catalog: Pokémon TCG sets and cards.
-- Populated by the metadata loader from the Pokémon TCG API (sets since market.catalog.since-release-date).

CREATE TABLE card_set (
    set_id        VARCHAR(64) PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    series        VARCHAR(255),
    printed_total INTEGER,
    total         INTEGER,
    release_date  DATE,
    logo_url      TEXT,
    symbol_url    TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE card (
    card_id     VARCHAR(64) PRIMARY KEY,
    set_id      VARCHAR(64) NOT NULL REFERENCES card_set (set_id),
    name        VARCHAR(255) NOT NULL,
    number      VARCHAR(32),
    rarity      VARCHAR(64),
    supertype   VARCHAR(64),
    image_small TEXT,
    image_large TEXT,
    game        VARCHAR(32) NOT NULL DEFAULT 'POKEMON',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_card_set_id ON card (set_id);
CREATE INDEX idx_card_rarity ON card (rarity);
-- Case-insensitive name search for GET /api/cards?q=
CREATE INDEX idx_card_name_lower ON card (lower(name));
