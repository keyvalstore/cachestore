/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package cachestore

import (
	"github.com/patrickmn/go-cache"
	"reflect"
	"time"
)

func OpenDatabase(options ...Option) *cache.Cache {

	conf := &Config{
		DefaultExpiration: cache.NoExpiration,
		CleanupInterval:  time.Hour,
	}

	for _, opt := range options {
		opt.apply(conf)
	}

	return cache.New(conf.DefaultExpiration, conf.CleanupInterval)
}

func ObjectType() reflect.Type {
	return CacheStoreClass
}



