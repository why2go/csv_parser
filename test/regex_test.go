package test

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegex(t *testing.T) {
	sliceRegex := regexp.MustCompile(`^\[\[[\w:-]+\]\]$`)
	mapRegex := regexp.MustCompile(`^{{[\w-]+:[\w-:]+}}$`)

	assert.True(t, sliceRegex.MatchString("[[slice1]]"))
	assert.False(t, sliceRegex.MatchString("[slice1]]"))
	assert.True(t, sliceRegex.MatchString("[[0:slice1]]"))

	assert.False(t, mapRegex.MatchString("{{map1}}"))
	assert.False(t, mapRegex.MatchString("{{ map1 }}"))
	assert.False(t, mapRegex.MatchString("{map1}"))
	assert.True(t, mapRegex.MatchString("{{map:key1}}"))
	assert.True(t, mapRegex.MatchString("{{map:key1:subkey1}}"))
}
