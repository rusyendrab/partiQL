SELECT e.InvoiceNumber, e.CreatedTime, e.StoreID, e.PosID, e.CashierID, e.CustomerType, e.CustomerCardNo,e.TotalAmount,
e.NumberOfItems, e.PaymentMethod, e.TaxableAmount, e.CGST, e.SGST, e.CGST, e.CESS, e.CGST,e.DeliveryType,
a.AddressLine as DeliveryAddress_AddressLine,
a.City,
a.State,
a.PinCode,
a.ContactNumber,
li.ItemCode,
li.ItemDescription,
li.ItemPrice,
li.ItemQty,
li.TotalValue
from invoice e
LEFT CROSS JOIN e.DeliveryAddress as a
LEFT CROSS JOIN e.InvoiceLineItems as li
