SELECT e.InvoiceNumber, e.CreatedTime, e.StoreID, e.PosID, e.CashierID, e.CustomerType, e.CustomerCardNo,e.TotalAmount,
e.NumberOfItems, e.PaymentMethod, e.TaxableAmount, e.CGST, e.SGST, e.CGST, e.CESS, e.CGST,e.DeliveryType,
e.DeliveryAddress_AddressLine,
e.DeliveryAddress_City,
e.DeliveryAddress_State,
e.DeliveryAddress_PinCode,
e.DeliveryAddress_ContactNumber,
e.InvoiceLineItems_ItemCode,
e.InvoiceLineItems_ItemDescription,
e.InvoiceLineItems_ItemPrice,
e.InvoiceLineItems_ItemQty,
e.InvoiceLineItems_TotalValue
from invoice i LEFT CROSS JOIN i.invoiceFlat AS e