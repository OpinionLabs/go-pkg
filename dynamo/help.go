package dynamo

import (
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const MIN = 0.0000000001

const (
	TimeoutMS = int64(3000)
)

func consumedInfo(cs []types.ConsumedCapacity) string {
	var details []string
	for i := range cs {
		detailsIn := ""
		if cs[i].CapacityUnits != nil {
			detailsIn += fmt.Sprintf(" total consume: %0.4f | ", *cs[i].CapacityUnits)
		}
		for k, v := range cs[i].LocalSecondaryIndexes {
			if v.CapacityUnits != nil {
				detailsIn += fmt.Sprintf(" local index %s consume: %0.4f | ", k, *v.CapacityUnits)
			}
		}
		if cs[i].TableName != nil {
			detailsIn += " table:" + (*cs[i].TableName) + " | "
		}
		details = append(details, detailsIn)
	}
	return strings.Join(details, " ; ")
}

// getUpdateExpression gets the update expression, parameter description:
//
//  sets:    collection of attributes to update
//  removes: collection of attributes to delete
//  names:   mapping from name placeholders to attribute names in update expression
//  values:  mapping from value placeholders to attribute values in update expression
func getUpdateExpression(sets, removes map[string]any, names map[string]string, values map[string]any) string {
	setExpression := ""
	index := 1
	for k, v := range sets {
		name := fmt.Sprintf("#%s%d", NamePlaceholderUpdatePrefix, index)
		value := fmt.Sprintf(":%s%d", ValuePlaceholderUpdatePrefix, index)
		names[name] = k
		values[value] = v
		index += 1

		if setExpression != "" {
			setExpression += ","
		}
		setExpression += fmt.Sprintf("%s=%s", name, value)
	}

	removeExpression := ""
	index = 1
	for k := range removes {
		name := fmt.Sprintf("#%s%d", NamePlaceholderRemovePrefix, index)
		names[name] = k
		index += 1

		if removeExpression != "" {
			removeExpression += ","
		}
		removeExpression += name
	}

	updateExpression := ""
	if setExpression != "" {
		updateExpression += " SET "
		updateExpression += setExpression
	}
	if removeExpression != "" {
		updateExpression += " REMOVE "
		updateExpression += removeExpression
	}
	return updateExpression
}

// getConditionExpression gets the condition expression
func getConditionExpression(conditions Conditions, names map[string]string, values map[string]any) string {
	var conds []string

	// Check conditions for attribute existence
	if len(conditions.AttributeExists) > 0 {
		index := 1
		for attrName := range conditions.AttributeExists {
			namePlaceholder := fmt.Sprintf("#%s%d", NamePlaceholderAttrExistsPrefix, index)
			conds = append(conds, fmt.Sprintf("attribute_exists(%s)", namePlaceholder))
			names[namePlaceholder] = attrName
			index += 1
		}
	}

	// Check conditions for attribute non-existence
	if len(conditions.AttributeNotExists) > 0 {
		index := 1
		for attrName := range conditions.AttributeNotExists {
			namePlaceholder := fmt.Sprintf("#%s%d", NamePlaceholderAttrNotExistsPrefix, index)
			conds = append(conds, fmt.Sprintf("attribute_not_exists(%s)", namePlaceholder))
			names[namePlaceholder] = attrName
			index += 1
		}
	}

	// Check conditions for attribute equality
	if len(conditions.AttributeEqual) > 0 {
		index := 1
		for attrName, attrVal := range conditions.AttributeEqual {
			namePlaceholder := fmt.Sprintf("#%s%d", NamePlaceholderAttrEqualPrefix, index)
			valuePlaceholder := fmt.Sprintf(":%s%d", ValuePlaceholderAttrEqualPrefix, index)
			conds = append(conds, fmt.Sprintf("%s=%s", namePlaceholder, valuePlaceholder))
			names[namePlaceholder] = attrName
			values[valuePlaceholder] = attrVal
			index += 1
		}
	}

	// Check conditions for attribute non-existence or equality
	if len(conditions.AttributeNotExistsOrEqual) > 0 {
		index := 1
		for attrName, attrVal := range conditions.AttributeNotExistsOrEqual {
			namePlaceholder := fmt.Sprintf("#%s%d", NamePlaceholderAttrNotExistsOrEqual, index)
			valuePlaceholder := fmt.Sprintf(":%s%d", ValuePlaceholderAttrNotExistsOrEqual, index)
			conds = append(conds, fmt.Sprintf("(attribute_not_exists(%s) or %s=%s)", namePlaceholder, namePlaceholder, valuePlaceholder))
			names[namePlaceholder] = attrName
			values[valuePlaceholder] = attrVal
			index += 1
		}
	}

	return strings.Join(conds, " and ")
}

// // isIdempotentErr checks if the error indicates an idempotent request duplication
// func isIdempotentErr(err error) bool {
// 	return false // Temporarily disable idempotency check
// 	opErr, ok := err.(*smithy.OperationError)
// 	if !ok {
// 		return false
// 	}
// 	httpErr, ok := opErr.Err.(*awshttp.ResponseError)
// 	if !ok || httpErr == nil {
// 		return false
// 	}
// 	_, ok = httpErr.Err.(*types.IdempotentParameterMismatchException)
// 	return ok
// }


func IsEqual(f1, f2 float64) bool {
	return math.Abs(f1-f2) < MIN
}

// Handle small values, as dynamodb reports errors for very small values
func Float64(f float64) float64 {
	if math.IsNaN(f) {
		return 0.0
	}
	if math.IsInf(f, 0) {
		return 0
	}
	if IsEqual(f, 0.0) {
		return 0.0
	}
	return f
}

// KvSetsFromItem fills fields from item into sets
// item must be a struct
// key uses dynamodbav tag from item
func KvSetsFromItem(keys, sets map[string]interface{}, item interface{}) error {
	oldLen := len(sets)
	typ := reflect.TypeOf(item)
	val := reflect.ValueOf(item)
	if typ.Kind() != reflect.Struct {
		// Not a struct, return directly
		return fmt.Errorf("item is not struct, %+v", item)
	}
	for i := 0; i < val.NumField(); i++ {
		key := typ.Field(i).Tag.Get("dynamodbav")
		key = strings.Split(key, ",")[0]
		if key == "" {
			continue
		}
		if _, ok := keys[key]; ok {
			// Filter out partition key and sort key
			continue
		}

		val := val.Field(i)
		if val.Kind() == reflect.Float64 {
			// Prevent floating point numbers from being too small and triggering dynamodb limitation bugs
			val := Float64(val.Interface().(float64))
			sets[key] = val
		} else {
			// Other field types
			sets[key] = val.Interface()
		}
	}
	if len(sets) == oldLen {
		return fmt.Errorf("field not exist, %+v", item)
	}
	return nil
}
