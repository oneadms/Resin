package proxy

import (
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/sagernet/sing-box/adapter"
)

type routedOutbound struct {
	Route          routing.RouteResult
	Outbound       adapter.Outbound
	StaticProxyURL string
}

type platformLookup interface {
	GetPlatform(id string) (*platform.Platform, bool)
}

func resolveRoutedOutbound(
	router *routing.Router,
	pool outbound.PoolAccessor,
	platformName string,
	account string,
	target string,
) (routedOutbound, *ProxyError) {
	result, err := router.RouteRequest(platformName, account, target)
	if err != nil {
		return routedOutbound{}, mapRouteError(err)
	}

	entry, ok := pool.GetEntry(result.NodeHash)
	if !ok {
		return routedOutbound{}, ErrNoAvailableNodes
	}
	obPtr := entry.Outbound.Load()
	if obPtr == nil {
		return routedOutbound{}, ErrNoAvailableNodes
	}
	staticProxyURL := ""
	if lookup, ok := pool.(platformLookup); ok {
		if plat, ok := lookup.GetPlatform(result.PlatformID); ok && plat != nil {
			staticProxyURL = plat.StaticProxyURL
		}
	}

	return routedOutbound{
		Route:          result,
		Outbound:       *obPtr,
		StaticProxyURL: staticProxyURL,
	}, nil
}
