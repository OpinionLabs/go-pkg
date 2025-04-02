package dynamo

import (
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGetUpdateExpression_Case0 tests getting update expressions
func TestGetUpdateExpression_Case0(t *testing.T) {
	tests := []struct {
		GivenSets        map[string]any
		GivenRemoves     map[string]any
		ExpectExpression string
		ExpectNameCnt    int
		ExpectValueCnt   int
	}{
		{
			GivenSets:        map[string]any{},
			ExpectExpression: "",
			ExpectNameCnt:    0,
			ExpectValueCnt:   0,
		},
		{
			GivenSets: map[string]any{
				"attr1": 1,
			},
			ExpectExpression: " SET #un1=:uv1",
			ExpectNameCnt:    1,
			ExpectValueCnt:   1,
		},
		{
			GivenSets: map[string]any{
				"attr1": 1,
				"attr2": "b",
			},
			ExpectExpression: " SET #un1=:uv1,#un2=:uv2",
			ExpectNameCnt:    2,
			ExpectValueCnt:   2,
		},
		{
			GivenSets: map[string]any{
				"attr1": 1,
				"attr2": "b",
			},
			GivenRemoves: map[string]any{
				"attr3": struct{}{},
				"attr4": struct{}{},
			},
			ExpectExpression: " SET #un1=:uv1,#un2=:uv2" +
				" REMOVE #rn1,#rn2",
			ExpectNameCnt:  4,
			ExpectValueCnt: 2,
		},
	}

	assert := require.New(t)
	for _, tt := range tests {
		names, values := make(map[string]string), make(map[string]any)
		getExpression := getUpdateExpression(tt.GivenSets, tt.GivenRemoves, names, values)
		assert.Equal(tt.ExpectExpression, getExpression)
		assert.Equal(tt.ExpectNameCnt, len(names))
		assert.Equal(tt.ExpectValueCnt, len(values))
		log.Println("\tnames:", names, ", values:", values)
	}
}

func TestGetConditionExpression_Case0(t *testing.T) {
	tests := []struct {
		GivenConditions  Conditions
		ExpectExpression string
		ExpectNameCnt    int
		ExpectValueCnt   int
	}{
		{
			ExpectExpression: "",
			ExpectNameCnt:    0,
			ExpectValueCnt:   0,
		},
		{
			GivenConditions: Conditions{
				AttributeExists: map[string]any{
					"attr1": struct{}{},
					"attr2": struct{}{},
				},
			},
			ExpectExpression: "attribute_exists(#aesn1) and attribute_exists(#aesn2)",
			ExpectNameCnt:    2,
			ExpectValueCnt:   0,
		},
		{
			GivenConditions: Conditions{
				AttributeExists: map[string]any{
					"attr1": struct{}{},
					"attr2": struct{}{},
				},
				AttributeNotExists: map[string]any{
					"attr3": struct{}{},
					"attr4": struct{}{},
				},
			},
			ExpectExpression: "attribute_exists(#aesn1) and attribute_exists(#aesn2)" +
				" and attribute_not_exists(#anesn1) and attribute_not_exists(#anesn2)",
			ExpectNameCnt:  4,
			ExpectValueCnt: 0,
		},
		{
			GivenConditions: Conditions{
				AttributeExists: map[string]any{
					"attr1": struct{}{},
					"attr2": struct{}{},
				},
				AttributeNotExists: map[string]any{
					"attr3": struct{}{},
					"attr4": struct{}{},
				},
				AttributeEqual: map[string]any{
					"attr5": 11,
					"attr6": "aa",
				},
				AttributeNotExistsOrEqual: map[string]any{
					"attr7": 22,
					"attr8": "bb",
				},
			},
			ExpectExpression: "attribute_exists(#aesn1) and attribute_exists(#aesn2)" +
				" and attribute_not_exists(#anesn1) and attribute_not_exists(#anesn2)" +
				" and #aeqn1=:aeqv1 and #aeqn2=:aeqv2" +
				" and (attribute_not_exists(#anesoeqn1) or #anesoeqn1=:anesoeqv1) and (attribute_not_exists(#anesoeqn2) or #anesoeqn2=:anesoeqv2)",
			ExpectNameCnt:  8,
			ExpectValueCnt: 4,
		},
	}

	assert := require.New(t)
	for _, tt := range tests {
		names, values := make(map[string]string), make(map[string]any)
		getExpression := getConditionExpression(tt.GivenConditions, names, values)
		assert.Equal(tt.ExpectExpression, getExpression)
		assert.Equal(tt.ExpectNameCnt, len(names))
		assert.Equal(tt.ExpectValueCnt, len(values))
		log.Println("\tnames:", names, ", values:", values)
	}
}
