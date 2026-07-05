package topology

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Resinat/Resin/internal/node"
)

type rawDetourOptions struct {
	Detour string `json:"detour"`
}

// ResolveNodeDetourChain resolves and validates the detour chain for a node.
// The returned slice starts with the node's immediate detour and continues
// outward. An empty slice means the node has no detour.
func (p *GlobalNodePool) ResolveNodeDetourChain(hash node.Hash) ([]node.Hash, error) {
	if p == nil {
		return nil, fmt.Errorf("node pool is nil")
	}
	return p.resolveNodeDetourChain(hash, map[node.Hash]bool{}, nil)
}

func (p *GlobalNodePool) resolveNodeDetourChain(
	hash node.Hash,
	visiting map[node.Hash]bool,
	chain []node.Hash,
) ([]node.Hash, error) {
	if visiting[hash] {
		return nil, fmt.Errorf("detour cycle includes node %s", hash.Hex())
	}
	entry, ok := p.GetEntry(hash)
	if !ok || entry == nil {
		return nil, fmt.Errorf("node %s not found", hash.Hex())
	}

	detourTag, err := nodeDetourTag(entry.RawOptions)
	if err != nil {
		return nil, fmt.Errorf("node %s: %w", hash.Hex(), err)
	}
	if detourTag == "" {
		return chain, nil
	}

	target, err := p.resolveDetourTag(detourTag)
	if err != nil {
		return nil, err
	}
	if target == hash {
		return nil, fmt.Errorf("detour self-reference: node %s uses tag %q", hash.Hex(), detourTag)
	}
	if visiting[target] {
		return nil, fmt.Errorf("detour cycle at tag %q", detourTag)
	}

	visiting[hash] = true
	chain = append(chain, target)
	chain, err = p.resolveNodeDetourChain(target, visiting, chain)
	delete(visiting, hash)
	return chain, err
}

func nodeDetourTag(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var opts rawDetourOptions
	if err := json.Unmarshal(raw, &opts); err != nil {
		return "", fmt.Errorf("parse outbound detour: %w", err)
	}
	return strings.TrimSpace(opts.Detour), nil
}

func (p *GlobalNodePool) resolveDetourTag(tag string) (node.Hash, error) {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return node.Zero, fmt.Errorf("detour tag is empty")
	}

	subLookup := p.MakeSubLookup()
	matches := map[node.Hash]struct{}{}
	p.nodes.Range(func(hash node.Hash, entry *node.NodeEntry) bool {
		if entry == nil {
			return true
		}
		for _, subID := range entry.SubscriptionIDs() {
			_, enabled, tags, ok := subLookup(subID, hash)
			if !ok || !enabled {
				continue
			}
			for _, candidate := range tags {
				if candidate == tag {
					matches[hash] = struct{}{}
				}
			}
		}
		return true
	})

	switch len(matches) {
	case 0:
		return node.Zero, fmt.Errorf("detour tag %q not found", tag)
	case 1:
		for hash := range matches {
			return hash, nil
		}
	}

	hashes := make([]string, 0, len(matches))
	for hash := range matches {
		hashes = append(hashes, hash.Hex())
	}
	sort.Strings(hashes)
	return node.Zero, fmt.Errorf("detour tag %q is ambiguous: %s", tag, strings.Join(hashes, ", "))
}
