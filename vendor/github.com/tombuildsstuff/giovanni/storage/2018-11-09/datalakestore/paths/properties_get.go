package paths

import (
	"context"
	"log"
	"net/http"

	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/validation"
	"github.com/tombuildsstuff/giovanni/storage/internal/endpoints"
)

type GetPropertiesResponse struct {
	autorest.Response

	// A map of base64-encoded strings to store as user-defined properties with the File System
	// Note that items may only contain ASCII characters in the ISO-8859-1 character set.
	// This automatically gets converted to a comma-separated list of name and
	// value pairs before sending to the API
	Properties map[string]string

	ResourceType PathResource
}

// GetProperties gets the properties for a Data Lake Store Gen2 Path in a FileSystem within a Storage Account
func (client Client) GetProperties(ctx context.Context, accountName string, fileSystemName string, path string) (result GetPropertiesResponse, err error) {
	if accountName == "" {
		return result, validation.NewError("datalakestore.Client", "GetProperties", "`accountName` cannot be an empty string.")
	}
	if fileSystemName == "" {
		return result, validation.NewError("datalakestore.Client", "GetProperties", "`fileSystemName` cannot be an empty string.")
	}

	req, err := client.GetPropertiesPreparer(ctx, accountName, fileSystemName, path)
	if err != nil {
		err = autorest.NewErrorWithError(err, "datalakestore.Client", "GetProperties", nil, "Failure preparing request")
		return
	}

	resp, err := client.GetPropertiesSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "datalakestore.Client", "GetProperties", resp, "Failure sending request")
		return
	}

	result, err = client.GetPropertiesResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "datalakestore.Client", "GetProperties", resp, "Failure responding to request")
	}

	return
}

// GetPropertiesPreparer prepares the GetProperties request.
func (client Client) GetPropertiesPreparer(ctx context.Context, accountName string, fileSystemName string, path string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"fileSystemName": autorest.Encode("path", fileSystemName),
		"path":           autorest.Encode("path", path),
	}

	queryParameters := map[string]interface{}{
		// "action": autorest.Encode("query", "getAccessControl"),
		"action": autorest.Encode("query", "getStatus"),
	}

	headers := map[string]interface{}{
		"x-ms-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsHead(),
		autorest.WithBaseURL(endpoints.GetDataLakeStoreEndpoint(client.BaseURI, accountName)),
		autorest.WithPathParameters("/{fileSystemName}/{path}", pathParameters),
		autorest.WithQueryParameters(queryParameters),
		autorest.WithHeaders(headers))

	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// GetPropertiesSender sends the GetProperties request. The method will close the
// http.Response Body if it receives an error.
func (client Client) GetPropertiesSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
}

// GetPropertiesResponder handles the response to the GetProperties request. The method always
// closes the http.Response Body.
func (client Client) GetPropertiesResponder(resp *http.Response) (result GetPropertiesResponse, err error) {
	if resp != nil && resp.Header != nil {

		propertiesRaw := resp.Header.Get("x-ms-properties")
		var properties *map[string]string
		properties, err = parseProperties(propertiesRaw)
		if err != nil {
			return
		}
		result.Properties = *properties

		resourceTypeRaw := resp.Header.Get("x-ms-resource-type")
		var resourceType PathResource
		if resourceTypeRaw != "" {
			resourceType, err = parsePathResource(resourceTypeRaw)
			if err != nil {
				return
			}
			result.ResourceType = resourceType
		}
	}
	log.Printf("*****: %v\n", resp)
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}

	return
}
