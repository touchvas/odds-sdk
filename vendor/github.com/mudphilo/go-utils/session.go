package library

import (
	"encoding/json"
	"fmt"
	session "github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
	"net/http"
)

func GetJSONRawBody(c echo.Context) map[string]interface{} {

	request := make(map[string]interface{})
	err := json.NewDecoder(c.Request().Body).Decode(&request)
	if err != nil {
		return nil
	}

	return request
}

func GetSessionValues(c echo.Context) (clientID int64, userID int64, roleID int64, payload map[string]interface{},httpStatus int, err error ) {

	sess, err := session.Get("session", c)
	if err != nil {
		return 0, 0, 0, nil, http.StatusUnprocessableEntity, err //fmt.Errorf("session timeout")
	}

	// Set user as authenticated
	userID,_ = GetInt64Value(sess.Values["user_id"],0)
	clientID,_ = GetInt64Value(sess.Values["client_id"],0)
	roleID,_ = GetInt64Value(sess.Values["role_id"],0)

	if clientID == 0 || userID == 0 {
		return 0, 0, 0, nil, http.StatusUnprocessableEntity, fmt.Errorf("session timeout")
	}

	return clientID, userID, roleID, GetJSONRawBody(c), http.StatusOK, nil
}

func GetSessionOnly(c echo.Context) (clientID int64, userID int64, roleID int64,httpStatus int, err error ) {

	sess, err := session.Get("session", c)
	if err != nil {
		return 0, 0, 0, http.StatusUnprocessableEntity, err //fmt.Errorf("session timeout")
	}

	// Set user as authenticated
	userID,_ = GetInt64Value(sess.Values["user_id"],0)
	clientID,_ = GetInt64Value(sess.Values["client_id"],0)
	roleID,_ = GetInt64Value(sess.Values["role_id"],0)

	if clientID == 0 || userID == 0 {
		return 0, 0, 0, http.StatusUnprocessableEntity, fmt.Errorf("session timeout")
	}

	return clientID, userID, roleID, http.StatusOK, nil
}

func GetValuesOnly(c echo.Context) (payload map[string]interface{},httpStatus int, err error ) {

	return GetJSONRawBody(c), http.StatusOK, nil
}
